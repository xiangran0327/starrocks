// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/rowset/segment_v2/page_io.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "storage/rowset/page_io.h"

#include <cstring>
#include <string>
#include <string_view>

#include "cache/object_cache/page_cache.h"
#include "column/column.h"
#include "common/logging.h"
#include "fs/fs.h"
#include "gutil/strings/substitute.h"
#include "storage/rowset/storage_page_decoder.h"
#include "util/coding.h"
#include "util/compression/block_compression.h"
#include "util/crc32c.h"
#include "util/faststring.h"
#include "util/runtime_profile.h"
#include "util/scoped_cleanup.h"

namespace starrocks {

using strings::Substitute;

Status PageIO::compress_page_body(const BlockCompressionCodec* codec, double min_space_saving,
                                  const std::vector<Slice>& body, faststring* compressed_body) {
    size_t uncompressed_size = Slice::compute_total_size(body);
    auto cleanup = MakeScopedCleanup([&]() { compressed_body->clear(); });
    if (codec != nullptr && codec->exceed_max_input_size(uncompressed_size)) {
        compressed_body->clear();
        return Status::OK();
    }
    if (codec != nullptr && uncompressed_size > 0) {
        if (use_compression_pool(codec->type())) {
            Slice compressed_slice;
            RETURN_IF_ERROR(
                    codec->compress(body, &compressed_slice, true, uncompressed_size, compressed_body, nullptr));
        } else {
            compressed_body->resize(codec->max_compressed_len(uncompressed_size));
            Slice compressed_slice(*compressed_body);
            RETURN_IF_ERROR(codec->compress(body, &compressed_slice));
            compressed_body->resize(compressed_slice.get_size());
        }
        double space_saving = 1.0 - static_cast<double>(compressed_body->size()) / uncompressed_size;
        // return compressed body only when it saves more than min_space_saving
        if (space_saving > 0 && space_saving >= min_space_saving) {
            compressed_body->shrink_to_fit();
            cleanup.cancel();
            return Status::OK();
        }
    }
    return Status::OK();
}

Status PageIO::write_page(WritableFile* wfile, const std::vector<Slice>& body, const PageFooterPB& footer,
                          PagePointer* result) {
    // sanity check of page footer
    CHECK(footer.has_type()) << "type must be set";
    CHECK(footer.has_uncompressed_size()) << "uncompressed_size must be set";
    switch (footer.type()) {
    case DATA_PAGE:
        CHECK(footer.has_data_page_footer());
        break;
    case INDEX_PAGE:
        CHECK(footer.has_index_page_footer());
        break;
    case DICTIONARY_PAGE:
        CHECK(footer.has_dict_page_footer());
        break;
    case SHORT_KEY_PAGE:
        CHECK(footer.has_short_key_page_footer());
        break;
    default:
        CHECK(false) << "Invalid page footer type: " << footer.type();
        break;
    }

    std::string footer_buf; // serialized footer + footer size
    footer.SerializeToString(&footer_buf);
    put_fixed32_le(&footer_buf, static_cast<uint32_t>(footer_buf.size()));

    std::vector<Slice> page = body;
    page.emplace_back(footer_buf);

    // checksum
    uint8_t checksum_buf[sizeof(uint32_t)];
    uint32_t checksum = crc32c::Value(page);
    encode_fixed32_le(checksum_buf, checksum);
    page.emplace_back(checksum_buf, sizeof(uint32_t));

    uint64_t offset = wfile->size();
    RETURN_IF_ERROR(wfile->appendv(&page[0], page.size()));

    result->offset = offset;
    result->size = wfile->size() - offset;
    return Status::OK();
}

// The unique key identifying entries in the page cache.
// Each cached page corresponds to a specific offset within
// a file.
//
// TODO(zc): Now we use file name(std::string) as a part of key,
//  which is not efficient. We should make it better later
std::string encode_cache_key(const std::string& fname, int64_t offset) {
    std::string str;
    str.reserve(fname.size() + sizeof(offset));
    str.append(fname);
    str.append((char*)&offset, sizeof(offset));
    return str;
}

Status PageIO::read_and_decompress_page(const PageReadOptions& opts, PageHandle* handle, Slice* body,
                                        PageFooterPB* footer) {
    // the function will be used by query or load, current load is not allowed to fail when memory reach the limit,
    // so don't check when tls_thread_state.check is set to false
    CHECK_MEM_LIMIT("read and decompress page");

    opts.sanity_check();
    opts.stats->total_pages_num++;

    auto cache = StoragePageCache::instance();
    bool page_cache_available = (cache != nullptr) && cache->available();
    PageCacheHandle cache_handle;
    std::string cache_key = encode_cache_key(opts.read_file->filename(), opts.page_pointer.offset);
    if (opts.use_page_cache && page_cache_available && cache->lookup(cache_key, &cache_handle)) {
        // we find page in cache, use it
        *handle = PageHandle(std::move(cache_handle));
        opts.stats->cached_pages_num++;
        // parse body and footer
        const auto* page = handle->data();
        uint32_t footer_length_offset = page->size() - 4;
        uint32_t footer_size = decode_fixed32_le(page->data() + footer_length_offset);
        uint32_t footer_offset = footer_length_offset - footer_size;
        std::string_view footer_buf{(const char*)page->data() + footer_offset, footer_size};
        if (!footer->ParseFromArray(footer_buf.data(), footer_buf.size())) {
            return Status::Corruption(
                    strings::Substitute("Bad page: invalid footer, read from page cache, file=$0, footer_size=$1",
                                        opts.read_file->filename(), footer_size));
        }
        *body = Slice(page->data(), footer_offset);
        return Status::OK();
    }

    // every page contains 4 bytes footer length and 4 bytes checksum
    const uint32_t page_size = opts.page_pointer.size;
    if (page_size < 8) {
        return Status::Corruption(
                strings::Substitute("Bad page: too small size ($0), file($1)", page_size, opts.read_file->filename()));
    }

    // hold compressed page at first, reset to decompressed page later
    // Allocate APPEND_OVERFLOW_MAX_SIZE more bytes to make append_strings_overflow work
    std::unique_ptr<std::vector<uint8_t>> page(new std::vector<uint8_t>());
    raw::stl_vector_resize_uninitialized(page.get(), page_size + Column::APPEND_OVERFLOW_MAX_SIZE, page_size - 4);
    Slice page_slice(page->data(), page_size);
    {
        SCOPED_RAW_TIMER(&opts.stats->io_ns);
        // todo override is_cache_hit
        RETURN_IF_ERROR(opts.read_file->read_at_fully(opts.page_pointer.offset, page_slice.data, page_slice.size));
        if (opts.read_file->is_cache_hit()) {
            ++opts.stats->pages_from_local_disk;
        }
        opts.stats->compressed_bytes_read_request += page_size;
        ++opts.stats->io_count_request;
    }

    if (opts.verify_checksum) {
        uint32_t expect = decode_fixed32_le((uint8_t*)page_slice.data + page_slice.size - 4);
        uint32_t actual = crc32c::Value(page_slice.data, page_slice.size - 4);
        if (expect != actual) {
            return Status::Corruption(
                    strings::Substitute("Bad page: checksum mismatch (actual=$0 vs expect=$1), file=$2 encrypted=$3",
                                        actual, expect, opts.read_file->filename(), opts.read_file->is_encrypted()));
        }
    }

    // remove checksum suffix
    page_slice.size -= 4;
    // parse and set footer
    uint32_t footer_size = decode_fixed32_le((uint8_t*)page_slice.data + page_slice.size - 4);
    if (!footer->ParseFromArray(page_slice.data + page_slice.size - 4 - footer_size, footer_size)) {
        return Status::Corruption(
                strings::Substitute("Bad page: invalid footer, read from disk, file=$0, footer_size=$1",
                                    opts.read_file->filename(), footer_size));
    }

    uint32_t body_size = page_slice.size - 4 - footer_size;
    if (body_size != footer->uncompressed_size()) { // need decompress body
        if (opts.codec == nullptr) {
            return Status::Corruption(strings::Substitute(
                    "Bad page: page is compressed but codec is NO_COMPRESSION, file=$0", opts.read_file->filename()));
        }
        SCOPED_RAW_TIMER(&opts.stats->decompress_ns);
        // Allocate APPEND_OVERFLOW_MAX_SIZE more bytes to make append_strings_overflow work
        std::unique_ptr<std::vector<uint8_t>> decompressed_page(new std::vector<uint8_t>());
        uint32_t decompressed_page_size = footer->uncompressed_size() + footer_size + 4;
        raw::stl_vector_resize_uninitialized(decompressed_page.get(),
                                             decompressed_page_size + Column::APPEND_OVERFLOW_MAX_SIZE,
                                             decompressed_page_size);

        // decompress page body
        Slice compressed_body(page_slice.data, body_size);
        Slice decompressed_body(decompressed_page->data(), footer->uncompressed_size());
        RETURN_IF_ERROR(opts.codec->decompress(compressed_body, &decompressed_body));
        if (decompressed_body.size != footer->uncompressed_size()) {
            return Status::Corruption(strings::Substitute(
                    "Bad page: record uncompressed size=$0 vs real decompressed size=$1, file=$2",
                    footer->uncompressed_size(), decompressed_body.size, opts.read_file->filename()));
        }
        // append footer and footer size
        memcpy(decompressed_body.data + decompressed_body.size, page_slice.data + body_size, footer_size + 4);
        // free memory of compressed page
        page = std::move(decompressed_page);
        page_slice = Slice(page->data(), decompressed_page_size);
        opts.stats->uncompressed_bytes_read += page_slice.size;
    } else {
        opts.stats->uncompressed_bytes_read += page_slice.size;
    }

    RETURN_IF_ERROR(StoragePageDecoder::decode_page(footer, footer_size + 4, opts.encoding_type, &page, &page_slice));

    *body = Slice(page_slice.data, page_slice.size - 4 - footer_size);
    if (opts.use_page_cache && page_cache_available) {
        // insert this page into cache and return the cache handle
        ObjectCacheWriteOptions opts;
        Status st = cache->insert(cache_key, page.get(), opts, &cache_handle);
        *handle = st.ok() ? PageHandle(std::move(cache_handle)) : PageHandle(page.get());
    } else {
        *handle = PageHandle(page.get());
    }
    page.release(); // memory now managed by handle
    return Status::OK();
}

} // namespace starrocks
