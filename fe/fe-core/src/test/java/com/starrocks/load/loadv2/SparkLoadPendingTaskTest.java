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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/load/loadv2/SparkLoadPendingTaskTest.java

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

package com.starrocks.load.loadv2;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.BrokerDesc;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.catalog.AggregateType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnId;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.SparkResource;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.LoadException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.load.BrokerFileGroup;
import com.starrocks.load.BrokerFileGroupAggInfo;
import com.starrocks.load.loadv2.etl.EtlJobConfig;
import com.starrocks.load.loadv2.etl.EtlJobConfig.EtlFileGroup;
import com.starrocks.load.loadv2.etl.EtlJobConfig.EtlIndex;
import com.starrocks.load.loadv2.etl.EtlJobConfig.EtlPartition;
import com.starrocks.load.loadv2.etl.EtlJobConfig.EtlPartitionInfo;
import com.starrocks.load.loadv2.etl.EtlJobConfig.EtlTable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.DataDescription;
import com.starrocks.sql.ast.PartitionKeyDesc;
import com.starrocks.sql.ast.PartitionNames;
import com.starrocks.sql.ast.PartitionValue;
import com.starrocks.sql.ast.SingleRangePartitionDesc;
import com.starrocks.sql.common.MetaUtils;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class SparkLoadPendingTaskTest {

    @Test
    public void testExecuteTask(@Injectable SparkLoadJob sparkLoadJob,
                                @Injectable SparkResource resource,
                                @Injectable BrokerDesc brokerDesc,
                                @Mocked GlobalStateMgr globalStateMgr,
                                @Injectable SparkLoadAppHandle handle,
                                @Injectable Database database,
                                @Injectable OlapTable table) throws LoadException {
        long dbId = 0L;
        long tableId = 1L;

        // columns
        List<Column> columns = Lists.newArrayList();
        columns.add(new Column("c1", Type.BIGINT, true, null, false, null, ""));
        Map<ColumnId, Column> idToColumn = Maps.newTreeMap(ColumnId.CASE_INSENSITIVE_ORDER);
        idToColumn.put(columns.get(0).getColumnId(), columns.get(0));

        // indexes
        Map<Long, List<Column>> indexIdToSchema = Maps.newHashMap();
        long indexId = 3L;
        indexIdToSchema.put(indexId, columns);

        // partition and distribution infos
        long partitionId = 2L;
        DistributionInfo distributionInfo = new HashDistributionInfo(2, Lists.newArrayList(columns.get(0)));
        PartitionInfo partitionInfo = new SinglePartitionInfo();
        Partition partition = new Partition(partitionId, 21,  "p1", null, distributionInfo);
        List<Partition> partitions = Lists.newArrayList(partition);

        // file group
        Map<BrokerFileGroupAggInfo.FileGroupAggKey, List<BrokerFileGroup>> aggKeyToFileGroups = Maps.newHashMap();
        List<BrokerFileGroup> brokerFileGroups = Lists.newArrayList();
        DataDescription desc = new DataDescription("testTable", null, Lists.newArrayList("abc.txt"),
                null, null, null, null, false, null);
        BrokerFileGroup brokerFileGroup = new BrokerFileGroup(desc);
        brokerFileGroups.add(brokerFileGroup);
        BrokerFileGroupAggInfo.FileGroupAggKey aggKey = new BrokerFileGroupAggInfo.FileGroupAggKey(tableId, null);
        aggKeyToFileGroups.put(aggKey, brokerFileGroups);

        new Expectations() {
            {
                globalStateMgr.getLocalMetastore().getDb(dbId);
                result = database;
                sparkLoadJob.getHandle();
                result = handle;
                GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(database.getId(), tableId);
                result = table;
                table.getPartitions();
                result = partitions;
                table.getIndexIdToSchema();
                result = indexIdToSchema;
                table.getDefaultDistributionInfo();
                result = distributionInfo;
                table.getSchemaHashByIndexId(indexId);
                result = 123;
                table.getPartitionInfo();
                result = partitionInfo;
                table.getPartition(partitionId);
                result = partition;
                table.getKeysTypeByIndexId(indexId);
                result = KeysType.DUP_KEYS;
                table.getBaseIndexId();
                result = indexId;
                table.getIdToColumn();
                result = idToColumn;
            }
        };

        String appId = "application_15888888888_0088";
        new MockUp<SparkEtlJobHandler>() {
            @Mock
            public void submitEtlJob(long loadJobId, String loadLabel, EtlJobConfig etlJobConfig,
                                     SparkResource resource, BrokerDesc brokerDesc,
                                     SparkLoadAppHandle handle,
                                     SparkPendingTaskAttachment attachment,
                                     Long sparkLoadSubmitTimeout) throws LoadException {
                attachment.setAppId(appId);
            }
        };

        SparkLoadPendingTask task = new SparkLoadPendingTask(sparkLoadJob, aggKeyToFileGroups, resource, brokerDesc);
        task.init();
        SparkPendingTaskAttachment attachment = Deencapsulation.getField(task, "attachment");
        Assertions.assertEquals(null, attachment.getAppId());
        task.executeTask();
        Assertions.assertEquals(appId, attachment.getAppId());
    }

    @Test
    public void testNoDb(@Injectable SparkLoadJob sparkLoadJob,
                         @Injectable SparkResource resource,
                         @Injectable BrokerDesc brokerDesc,
                         @Mocked GlobalStateMgr globalStateMgr) {
        assertThrows(LoadException.class, () -> {
            long dbId = 0L;

            new Expectations() {
                {
                    globalStateMgr.getLocalMetastore().getDb(dbId);
                    result = null;
                }
            };

            SparkLoadPendingTask task = new SparkLoadPendingTask(sparkLoadJob, null, resource, brokerDesc);
            task.init();
        });
    }

    @Test
    public void testNoTable(@Injectable SparkLoadJob sparkLoadJob,
                            @Injectable SparkResource resource,
                            @Injectable BrokerDesc brokerDesc,
                            @Mocked GlobalStateMgr globalStateMgr,
                            @Injectable Database database) {
        assertThrows(LoadException.class, () -> {
            long dbId = 0L;
            long tableId = 1L;

            Map<BrokerFileGroupAggInfo.FileGroupAggKey, List<BrokerFileGroup>> aggKeyToFileGroups = Maps.newHashMap();
            List<BrokerFileGroup> brokerFileGroups = Lists.newArrayList();
            DataDescription desc = new DataDescription("testTable", null, Lists.newArrayList("abc.txt"),
                    null, null, null, null, false, null);
            BrokerFileGroup brokerFileGroup = new BrokerFileGroup(desc);
            brokerFileGroups.add(brokerFileGroup);
            BrokerFileGroupAggInfo.FileGroupAggKey aggKey = new BrokerFileGroupAggInfo.FileGroupAggKey(tableId, null);
            aggKeyToFileGroups.put(aggKey, brokerFileGroups);

            new Expectations() {
                {
                    globalStateMgr.getLocalMetastore().getDb(dbId);
                    result = database;
                    GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(database.getId(), tableId);
                    result = null;
                }
            };

            SparkLoadPendingTask task = new SparkLoadPendingTask(sparkLoadJob, aggKeyToFileGroups, resource, brokerDesc);
            task.init();
        });
    }

    @Test
    public void testRangePartitionHashDistribution(@Injectable SparkLoadJob sparkLoadJob,
                                                   @Injectable SparkResource resource,
                                                   @Injectable BrokerDesc brokerDesc,
                                                   @Mocked GlobalStateMgr globalStateMgr,
                                                   @Injectable Database database,
                                                   @Injectable OlapTable table)
            throws LoadException, DdlException, AnalysisException {
        long dbId = 0L;
        long tableId = 1L;

        // c1 is partition column, c2 is distribution column
        List<Column> columns = Lists.newArrayList();
        columns.add(new Column("c1", Type.INT, true, null, false, null, ""));
        columns.add(new Column("c2", ScalarType.createVarchar(10), true, null, false, null, ""));
        columns.add(new Column("c3", Type.INT, false, AggregateType.SUM, false, null, ""));

        Map<ColumnId, Column> idToColumn = Maps.newTreeMap(ColumnId.CASE_INSENSITIVE_ORDER);
        for (Column column : columns) {
            idToColumn.put(column.getColumnId(), column);
        }

        // indexes
        Map<Long, List<Column>> indexIdToSchema = Maps.newHashMap();
        long index1Id = 3L;
        indexIdToSchema.put(index1Id, columns);
        long index2Id = 4L;
        indexIdToSchema.put(index2Id, Lists.newArrayList(columns.get(0), columns.get(2)));

        // partition and distribution info
        long partition1Id = 2L;
        long partition2Id = 5L;
        // partition3 is temporary partition
        long partition3Id = 6L;
        int distributionColumnIndex = 1;
        DistributionInfo distributionInfo =
                new HashDistributionInfo(3, Lists.newArrayList(columns.get(distributionColumnIndex)));
        Partition partition1 = new Partition(partition1Id, 21,  "p1", null,
                distributionInfo);
        Partition partition2 = new Partition(partition2Id, 51,  "p2", null,
                new HashDistributionInfo(4, Lists.newArrayList(columns.get(distributionColumnIndex))));
        Partition partition3 = new Partition(partition3Id, 61,  "tp3", null,
                distributionInfo);
        int partitionColumnIndex = 0;
        List<Partition> partitions = Lists.newArrayList(partition1, partition2);
        RangePartitionInfo partitionInfo =
                new RangePartitionInfo(Lists.newArrayList(columns.get(partitionColumnIndex)));
        PartitionKeyDesc partitionKeyDesc1 = new PartitionKeyDesc(Lists.newArrayList(new PartitionValue("10")));
        SingleRangePartitionDesc partitionDesc1 = new SingleRangePartitionDesc(false, "p1", partitionKeyDesc1, null);
        partitionDesc1.analyze(1, null);
        partitionInfo.handleNewSinglePartitionDesc(MetaUtils.buildIdToColumn(columns),
                partitionDesc1, partition1Id, false);
        PartitionKeyDesc partitionKeyDesc2 = new PartitionKeyDesc(Lists.newArrayList(new PartitionValue("20")));
        SingleRangePartitionDesc partitionDesc2 = new SingleRangePartitionDesc(false, "p2", partitionKeyDesc2, null);
        partitionDesc2.analyze(1, null);
        partitionInfo.handleNewSinglePartitionDesc(MetaUtils.buildIdToColumn(columns),
                partitionDesc2, partition2Id, false);
        PartitionKeyDesc partitionKeyDesc3 = new PartitionKeyDesc(Lists.newArrayList(new PartitionValue("10")));
        SingleRangePartitionDesc partitionDesc3 = new SingleRangePartitionDesc(false, "tp3", partitionKeyDesc1, null);
        partitionDesc3.analyze(1, null);
        partitionInfo.handleNewSinglePartitionDesc(MetaUtils.buildIdToColumn(columns),
                partitionDesc3, partition3Id, true);

        new Expectations() {
            {
                globalStateMgr.getLocalMetastore().getDb(dbId);
                result = database;
                GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(database.getId(), tableId);
                result = table;
                table.getPartitions();
                result = partitions;
                table.getIndexIdToSchema();
                result = indexIdToSchema;
                table.getDefaultDistributionInfo();
                result = distributionInfo;
                table.getSchemaHashByIndexId(index1Id);
                result = 123;
                table.getSchemaHashByIndexId(index2Id);
                result = 234;
                table.getPartitionInfo();
                result = partitionInfo;
                table.getPartition(partition1Id);
                result = partition1;
                table.getPartition(partition2Id);
                result = partition2;
                table.getPartition(partition3Id);
                result = partition3;
                table.getKeysTypeByIndexId(index1Id);
                result = KeysType.AGG_KEYS;
                table.getKeysTypeByIndexId(index2Id);
                result = KeysType.AGG_KEYS;
                table.getBaseIndexId();
                result = index1Id;
                table.getIdToColumn();
                result = idToColumn;
            }
        };

        // case 0: partition is null in load stmt
        // file group
        Map<BrokerFileGroupAggInfo.FileGroupAggKey, List<BrokerFileGroup>> aggKeyToFileGroups = Maps.newHashMap();
        List<BrokerFileGroup> brokerFileGroups = Lists.newArrayList();
        DataDescription desc = new DataDescription("testTable", null, Lists.newArrayList("abc.txt"),
                null, null, null, null, false, null);
        BrokerFileGroup brokerFileGroup = new BrokerFileGroup(desc);
        brokerFileGroups.add(brokerFileGroup);
        BrokerFileGroupAggInfo.FileGroupAggKey aggKey = new BrokerFileGroupAggInfo.FileGroupAggKey(tableId, null);
        aggKeyToFileGroups.put(aggKey, brokerFileGroups);

        // create pending task
        SparkLoadPendingTask task = new SparkLoadPendingTask(sparkLoadJob, aggKeyToFileGroups, resource, brokerDesc);
        EtlJobConfig etlJobConfig = Deencapsulation.getField(task, "etlJobConfig");
        Assertions.assertEquals(null, etlJobConfig);
        task.init();
        etlJobConfig = Deencapsulation.getField(task, "etlJobConfig");
        Assertions.assertTrue(etlJobConfig != null);

        // check table id
        Map<Long, EtlTable> idToEtlTable = etlJobConfig.tables;
        Assertions.assertEquals(1, idToEtlTable.size());
        Assertions.assertTrue(idToEtlTable.containsKey(tableId));

        // check indexes
        EtlTable etlTable = idToEtlTable.get(tableId);
        List<EtlIndex> etlIndexes = etlTable.indexes;
        Assertions.assertEquals(2, etlIndexes.size());
        Assertions.assertEquals(index1Id, etlIndexes.get(0).indexId);
        Assertions.assertEquals(index2Id, etlIndexes.get(1).indexId);

        // check base index columns
        EtlIndex baseIndex = etlIndexes.get(0);
        Assertions.assertTrue(baseIndex.isBaseIndex);
        Assertions.assertEquals(3, baseIndex.columns.size());
        for (int i = 0; i < columns.size(); i++) {
            Assertions.assertEquals(columns.get(i).getName(), baseIndex.columns.get(i).columnName);
        }
        Assertions.assertEquals("AGGREGATE", baseIndex.indexType);

        // check partitions
        EtlPartitionInfo etlPartitionInfo = etlTable.partitionInfo;
        Assertions.assertEquals("RANGE", etlPartitionInfo.partitionType);
        List<String> partitionColumns = etlPartitionInfo.partitionColumnRefs;
        Assertions.assertEquals(1, partitionColumns.size());
        Assertions.assertEquals(columns.get(partitionColumnIndex).getName(), partitionColumns.get(0));
        List<String> distributionColumns = etlPartitionInfo.distributionColumnRefs;
        Assertions.assertEquals(1, distributionColumns.size());
        Assertions.assertEquals(columns.get(distributionColumnIndex).getName(), distributionColumns.get(0));
        List<EtlPartition> etlPartitions = etlPartitionInfo.partitions;
        Assertions.assertEquals(2, etlPartitions.size());
        Assertions.assertEquals(21, etlPartitions.get(0).physicalPartitionId);
        Assertions.assertEquals(51, etlPartitions.get(1).physicalPartitionId);

        // check file group
        List<EtlFileGroup> etlFileGroups = etlTable.fileGroups;
        Assertions.assertEquals(1, etlFileGroups.size());

        // case 1: temporary partition in load stmt
        // file group
        aggKeyToFileGroups = Maps.newHashMap();
        brokerFileGroups = Lists.newArrayList();
        PartitionNames partitionNames = new PartitionNames(true, Lists.newArrayList("tp3"));
        desc = new DataDescription("testTable", partitionNames, Lists.newArrayList("abc.txt"),
                null, null, null, null, false, null);
        brokerFileGroup = new BrokerFileGroup(desc);
        brokerFileGroups.add(brokerFileGroup);
        aggKey = new BrokerFileGroupAggInfo.FileGroupAggKey(tableId, Lists.newArrayList(partition3Id));
        aggKeyToFileGroups.put(aggKey, brokerFileGroups);

        // create pending task
        task = new SparkLoadPendingTask(sparkLoadJob, aggKeyToFileGroups, resource, brokerDesc);
        task.init();

        etlJobConfig = Deencapsulation.getField(task, "etlJobConfig");
        Assertions.assertTrue(etlJobConfig != null);
        idToEtlTable = etlJobConfig.tables;
        etlTable = idToEtlTable.get(tableId);

        // check partitions
        etlPartitionInfo = etlTable.partitionInfo;
        Assertions.assertEquals("RANGE", etlPartitionInfo.partitionType);
        partitionColumns = etlPartitionInfo.partitionColumnRefs;
        Assertions.assertEquals(1, partitionColumns.size());
        Assertions.assertEquals(columns.get(partitionColumnIndex).getName(), partitionColumns.get(0));
        distributionColumns = etlPartitionInfo.distributionColumnRefs;
        Assertions.assertEquals(1, distributionColumns.size());
        Assertions.assertEquals(columns.get(distributionColumnIndex).getName(), distributionColumns.get(0));
        etlPartitions = etlPartitionInfo.partitions;
        Assertions.assertEquals(1, etlPartitions.size());
        Assertions.assertEquals(61, etlPartitions.get(0).physicalPartitionId);
    }

    private void internalTestBitmapMapping(SparkLoadPendingTask task, FunctionCallExpr expr, boolean expectLoadException) {
        try {
            task.checkBitmapMapping("col1", expr, true);
            if (expectLoadException) {
                Assertions.fail();
            }
        } catch (Exception e) {
            boolean isLoadException = e instanceof LoadException;
            if (isLoadException ^ expectLoadException) {
                Assertions.fail();
            }
        }
    }

    @Test
    public void testBitmapMapping(@Injectable SparkLoadJob sparkLoadJob,
                                  @Injectable SparkResource resource,
                                  @Injectable BrokerDesc brokerDesc,
                                  @Mocked GlobalStateMgr globalStateMgr) {
        SparkLoadPendingTask task = new SparkLoadPendingTask(sparkLoadJob, Maps.newHashMap(), resource, brokerDesc);

        {
            FunctionCallExpr expr = new FunctionCallExpr(FunctionSet.BITMAP_COUNT, Lists.newArrayList());
            internalTestBitmapMapping(task, expr, true);
        }

        {
            FunctionCallExpr expr = new FunctionCallExpr(FunctionSet.BITMAP_HASH64, Lists.newArrayList());
            internalTestBitmapMapping(task, expr, false);
        }
    }

}
