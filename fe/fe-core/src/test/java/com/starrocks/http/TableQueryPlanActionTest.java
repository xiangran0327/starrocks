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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/http/TableQueryPlanActionTest.java

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

package com.starrocks.http;

import com.starrocks.rpc.ConfigurableSerDesFactory;
import com.starrocks.server.WarehouseManager;
import com.starrocks.thrift.TQueryPlanInfo;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.thrift.TDeserializer;
import org.json.JSONObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Base64;
import java.util.Objects;

public class TableQueryPlanActionTest extends StarRocksHttpTestCase {

    private static final String PATH_URI = "/_query_plan";
    protected static String ES_TABLE_URL;
    protected static String WAREHOUSE_KEY = "warehouse";

    @Override
    protected void doSetUp() throws Exception {
        ES_TABLE_URL = "http://localhost:" + HTTP_PORT + "/api/" + DB_NAME + "/es_table";
    }

    @Test
    public void testQueryPlanAction() throws Exception {
        super.setUpWithCatalog();
        RequestBody body =
                RequestBody.create(JSON, "{ \"sql\" :  \" select k1 as alias_1,k2 from " + DB_NAME + "." + TABLE_NAME + " \" }");
        Request request = new Request.Builder()
                .post(body)
                .addHeader("Authorization", rootAuth)
                .addHeader(WAREHOUSE_KEY,  WarehouseManager.DEFAULT_WAREHOUSE_NAME)
                .url(URI + PATH_URI)
                .build();
        try (Response response = networkClient.newCall(request).execute()) {
            String respStr = Objects.requireNonNull(response.body()).string();
            JSONObject jsonObject = new JSONObject(respStr);
            System.out.println(respStr);
            Assertions.assertEquals(200, jsonObject.getInt("status"));

            JSONObject partitionsObject = jsonObject.getJSONObject("partitions");
            Assertions.assertNotNull(partitionsObject);
            for (String tabletKey : partitionsObject.keySet()) {
                JSONObject tabletObject = partitionsObject.getJSONObject(tabletKey);
                Assertions.assertNotNull(tabletObject.getJSONArray("routings"));
                Assertions.assertEquals(3, tabletObject.getJSONArray("routings").length());
                Assertions.assertEquals(testStartVersion, tabletObject.getLong("version"));
                Assertions.assertEquals(testSchemaHash, tabletObject.getLong("schemaHash"));

            }
            String queryPlan = jsonObject.getString("opaqued_query_plan");
            Assertions.assertNotNull(queryPlan);
            byte[] binaryPlanInfo = Base64.getDecoder().decode(queryPlan);
            TDeserializer deserializer = ConfigurableSerDesFactory.getTDeserializer();
            TQueryPlanInfo tQueryPlanInfo = new TQueryPlanInfo();
            deserializer.deserialize(tQueryPlanInfo, binaryPlanInfo);
            Assertions.assertEquals("alias_1", tQueryPlanInfo.output_names.get(0));
            expectThrowsNoException(() -> deserializer.deserialize(tQueryPlanInfo, binaryPlanInfo));
        }
    }

    @Test
    public void testNoSqlFailure() throws IOException {
        RequestBody body = RequestBody
                .create(JSON, "{}");
        Request request = new Request.Builder()
                .post(body)
                .addHeader("Authorization", rootAuth)
                .url(URI + PATH_URI)
                .build();
        try (Response response = networkClient.newCall(request).execute()) {
            String respStr = Objects.requireNonNull(response.body()).string();
            System.out.println(respStr);
            Assertions.assertNotNull(respStr);
            expectThrowsNoException(() -> new JSONObject(respStr));
            JSONObject jsonObject = new JSONObject(respStr);
            Assertions.assertEquals(400, jsonObject.getInt("status"));
            String exception = jsonObject.getString("exception");
            Assertions.assertNotNull(exception);
            Assertions.assertEquals("POST body must contains [sql] root object", exception);
        }
    }

    @Test
    public void testMalformedJson() throws IOException {
        RequestBody body =
                RequestBody.create(JSON, "{ \"sql\" :  \" select k1,k2 from " + DB_NAME + "." + TABLE_NAME + " \"");
        Request request = new Request.Builder()
                .post(body)
                .addHeader("Authorization", rootAuth)
                .url(ES_TABLE_URL + PATH_URI)
                .build();
        try (Response response = networkClient.newCall(request).execute()) {
            String respStr = Objects.requireNonNull(response.body()).string();
            Assertions.assertNotNull(respStr);
            expectThrowsNoException(() -> new JSONObject(respStr));
            JSONObject jsonObject = new JSONObject(respStr);
            Assertions.assertEquals(400, jsonObject.getInt("status"));
            String exception = jsonObject.getString("exception");
            Assertions.assertNotNull(exception);
            Assertions.assertTrue(exception.startsWith("malformed json"));
        }
    }

    @Test
    public void testNotOlapTableFailure() throws IOException {
        RequestBody body =
                RequestBody.create(JSON, "{ \"sql\" :  \" select k1,k2 from " + DB_NAME + ".es_table" + " \" }");
        Request request = new Request.Builder()
                .post(body)
                .addHeader("Authorization", rootAuth)
                .url(ES_TABLE_URL + PATH_URI)
                .build();
        try (Response response = networkClient.newCall(request).execute()) {
            String respStr = Objects.requireNonNull(response.body()).string();
            Assertions.assertNotNull(respStr);
            expectThrowsNoException(() -> new JSONObject(respStr));
            JSONObject jsonObject = new JSONObject(respStr);
            Assertions.assertEquals(403, jsonObject.getInt("status"));
            String exception = jsonObject.getString("exception");
            Assertions.assertNotNull(exception);
            Assertions.assertTrue(
                    exception.startsWith("Only support OlapTable, CloudNativeTable and MaterializedView currently"));
        }
    }

    @Test
    public void testQueryPlanActionPruneEmpty() throws Exception {
        super.setUpWithCatalog();


        String tableName = "test_empty_table";

        RequestBody body =
                RequestBody.create(JSON, "{ \"sql\" :  \" select k1,k2,k3 from " + DB_NAME + "." + tableName +
                        " where k3  > '2023-10-01 11:11:11' and k3 < '2023-10-02 11:11:11'" + " \" }");
        String uri = "http://localhost:" + HTTP_PORT + "/api/" + DB_NAME + "/test_empty_table";

        Request request = new Request.Builder()
                .post(body)
                .addHeader("Authorization", rootAuth)
                .url(uri + PATH_URI)
                .build();
        try (Response response = networkClient.newCall(request).execute()) {
            String respStr = Objects.requireNonNull(response.body()).string();
            Assertions.assertEquals("{\"partitions\":{},\"opaqued_query_plan\":\"\",\"status\":200}", respStr);
        }
    }

    @Test
    public void testInvalidWarehouseFailure() throws IOException {
        RequestBody body =
                RequestBody.create(JSON, "{ \"sql\" :  \" select k1 as alias_1,k2 from " + DB_NAME + "." + TABLE_NAME + " \" }");
        Request request = new Request.Builder()
                .post(body)
                .addHeader("Authorization", rootAuth)
                .addHeader(WAREHOUSE_KEY,  "non_existed_warehouse")
                .url(URI + PATH_URI)
                .build();
        try (Response response = networkClient.newCall(request).execute()) {
            String respStr = Objects.requireNonNull(response.body()).string();
            Assertions.assertNotNull(respStr);
            expectThrowsNoException(() -> new JSONObject(respStr));
            JSONObject jsonObject = new JSONObject(respStr);
            Assertions.assertEquals(400, jsonObject.getInt("status"));
            String exception = jsonObject.getString("exception");
            Assertions.assertNotNull(exception);
            Assertions.assertEquals("The warehouse parameter [non_existed_warehouse] is invalid", exception);
        }
    }
}
