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

package com.starrocks.backup;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.TableName;
import com.starrocks.analysis.TableRef;
import com.starrocks.backup.BackupJob.BackupJobState;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.FsBroker;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.util.UnitTestUtil;
import com.starrocks.metric.MetricRepo;
import com.starrocks.persist.EditLog;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.task.AgentBatchTask;
import com.starrocks.task.AgentTask;
import com.starrocks.task.AgentTaskExecutor;
import com.starrocks.task.AgentTaskQueue;
import com.starrocks.task.SnapshotTask;
import com.starrocks.task.UploadTask;
import com.starrocks.thrift.TBackend;
import com.starrocks.thrift.TFinishTaskRequest;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TTaskType;
import mockit.Delegate;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class BackupJobMaterializedViewTest {

    private BackupJob job;
    private Database db;

    private long dbId = 110;
    private long tblId = 120;
    private long partId = 130;
    private long idxId = 140;
    private long tabletId = 150;
    private long backendId = 11000;
    private long version = 160;
    private long repoId = 30001;
    private AtomicLong id = new AtomicLong(50001);
    private static final String MV_LABEL = "test_mv_backup_label";

    private static List<Path> pathsNeedToBeDeleted = Lists.newArrayList();

    @Mocked
    private GlobalStateMgr globalStateMgr;

    private MockBackupHandler backupHandler;

    private MockRepositoryMgr repoMgr;

    // Thread is not mockable in Jmockit, use subclass instead
    private final class MockBackupHandler extends BackupHandler {
        public MockBackupHandler(GlobalStateMgr globalStateMgr) {
            super(globalStateMgr);
        }

        @Override
        public RepositoryMgr getRepoMgr() {
            return repoMgr;
        }
    }

    // Thread is not mockable in Jmockit, use subclass instead
    private final class MockRepositoryMgr extends RepositoryMgr {
        public MockRepositoryMgr() {
            super();
        }

        @Override
        public Repository getRepo(long repoId) {
            return repo;
        }
    }

    @Mocked
    private EditLog editLog;

    private Repository repo = new Repository(repoId, "repo", false, "my_repo",
                new BlobStorage("broker", Maps.newHashMap()));

    @BeforeAll
    public static void start() {
        Config.tmp_dir = "./";
        File backupDir = new File(BackupHandler.TEST_BACKUP_ROOT_DIR.toString());
        if (!backupDir.exists()) {
            backupDir.mkdirs();
        }

        MetricRepo.init();
    }

    @AfterAll
    public static void end() throws IOException {
        for (Path path : pathsNeedToBeDeleted) {
            File backupDir = new File(path.toString());
            if (backupDir.exists()) {
                Files.walk(path, FileVisitOption.FOLLOW_LINKS).sorted(Comparator.reverseOrder()).map(Path::toFile)
                            .forEach(File::delete);
            }
        }
    }

    @BeforeEach
    public void setUp() {

        repoMgr = new MockRepositoryMgr();
        backupHandler = new MockBackupHandler(globalStateMgr);

        // Thread is unmockable after Jmockit version 1.48, so use reflection to set field instead.
        Deencapsulation.setField(globalStateMgr, "backupHandler", backupHandler);

        db = UnitTestUtil.createDbWithMaterializedView(dbId, tblId, partId, idxId, tabletId,
                    backendId, version, KeysType.DUP_KEYS);

        new Expectations(globalStateMgr) {
            {
                globalStateMgr.getLocalMetastore().getDb(anyLong);
                minTimes = 0;
                result = db;

                globalStateMgr.getNextId();
                minTimes = 0;
                result = id.getAndIncrement();

                globalStateMgr.getEditLog();
                minTimes = 0;
                result = editLog;

                globalStateMgr.getLocalMetastore().getTable("testDb", "unknown_mv");
                minTimes = 0;
                result = null;

                globalStateMgr.getLocalMetastore().getTable("testDb", "unknown_tbl");
                minTimes = 0;
                result = null;
            }
        };

        new Expectations() {
            {
                editLog.logBackupJob((BackupJob) any);
                minTimes = 0;
                result = new Delegate() {
                    public void logBackupJob(BackupJob job) {
                        System.out.println("log backup job: " + job);
                    }
                };
            }
        };

        new MockUp<AgentTaskExecutor>() {
            @Mock
            public void submit(AgentBatchTask task) {

            }
        };

        new MockUp<Repository>() {
            @Mock
            Status upload(String localFilePath, String remoteFilePath) {
                return Status.OK;
            }

            @Mock
            Status getBrokerAddress(Long beId, GlobalStateMgr globalStateMgr, List<FsBroker> brokerAddrs) {
                brokerAddrs.add(new FsBroker());
                return Status.OK;
            }
        };

        List<TableRef> tableRefs = Lists.newArrayList();
        // disorder
        tableRefs.add(new TableRef(new TableName(UnitTestUtil.DB_NAME, UnitTestUtil.MATERIALIZED_VIEW_NAME), null));
        tableRefs.add(new TableRef(new TableName(UnitTestUtil.DB_NAME, UnitTestUtil.TABLE_NAME), null));

        job = new BackupJob(MV_LABEL, dbId, UnitTestUtil.DB_NAME, tableRefs, 13600 * 1000, globalStateMgr, repo.getId());
        new Expectations(job) {
            {
                job.validateLocalFile(anyString);
                minTimes = 0;
                result = true;
            }
        };
    }

    @Disabled
    public void testRunNormal() {
        // 1.pending
        Assertions.assertEquals(BackupJobState.PENDING, job.getState());
        job.run();
        Assertions.assertEquals(Status.OK, job.getStatus());
        Assertions.assertEquals(BackupJobState.SNAPSHOTING, job.getState());

        BackupMeta backupMeta = job.getBackupMeta();
        Assertions.assertEquals(2, backupMeta.getTables().size());

        // test backup meta
        {
            {
                OlapTable backupTbl = (OlapTable) backupMeta.getTable(UnitTestUtil.TABLE_NAME);
                List<String> partNames = Lists.newArrayList(backupTbl.getPartitionNames());
                Assertions.assertNotNull(backupTbl);
                Assertions.assertEquals(backupTbl.getSignature(BackupHandler.SIGNATURE_VERSION, partNames, true),
                            ((OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                                        .getTable(db.getId(), tblId)).getSignature(BackupHandler.SIGNATURE_VERSION, partNames,
                                        true));
            }
            {
                OlapTable backupTbl = (OlapTable) backupMeta.getTable(UnitTestUtil.MATERIALIZED_VIEW_NAME);
                List<String> partNames = Lists.newArrayList(backupTbl.getPartitionNames());
                Assertions.assertNotNull(backupTbl);
                Assertions.assertEquals(backupTbl.getSignature(BackupHandler.SIGNATURE_VERSION, partNames, true),
                            ((OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                                        .getTable(db.getId(), tblId + 1)).getSignature(BackupHandler.SIGNATURE_VERSION, partNames,
                                        true));
            }
        }

        Assertions.assertEquals(2, AgentTaskQueue.getTaskNum());

        // 2. snapshoting
        job.run();
        Assertions.assertEquals(Status.OK, job.getStatus());
        Assertions.assertEquals(BackupJobState.SNAPSHOTING, job.getState());

        // 3. snapshot finished
        String snapshotPath = "/path/to/snapshot";
        List<String> snapshotFiles = Lists.newArrayList();
        snapshotFiles.add("1.dat");
        snapshotFiles.add("meta");
        TStatus taskStatus = new TStatus(TStatusCode.OK);
        TBackend tBackend = new TBackend("", 0, 1);

        // run task 1
        {
            AgentTask task = AgentTaskQueue.getTask(backendId, TTaskType.MAKE_SNAPSHOT, tabletId);
            Assertions.assertTrue(task instanceof SnapshotTask);
            SnapshotTask snapshotTask = (SnapshotTask) task;
            TFinishTaskRequest request = new TFinishTaskRequest(tBackend, TTaskType.MAKE_SNAPSHOT,
                        snapshotTask.getSignature(), taskStatus);
            request.setSnapshot_files(snapshotFiles);
            request.setSnapshot_path(snapshotPath);
            Assertions.assertTrue(job.finishTabletSnapshotTask(snapshotTask, request));
            job.run();
            Assertions.assertEquals(Status.OK, job.getStatus());
        }
        // run task 2
        {
            AgentTask task = AgentTaskQueue.getTask(backendId, TTaskType.MAKE_SNAPSHOT, tabletId + 1);
            Assertions.assertTrue(task instanceof SnapshotTask);
            SnapshotTask snapshotTask = (SnapshotTask) task;
            TFinishTaskRequest request = new TFinishTaskRequest(tBackend, TTaskType.MAKE_SNAPSHOT,
                        snapshotTask.getSignature(), taskStatus);
            request.setSnapshot_files(snapshotFiles);
            request.setSnapshot_path(snapshotPath);
            Assertions.assertTrue(job.finishTabletSnapshotTask(snapshotTask, request));
            job.run();
            Assertions.assertEquals(Status.OK, job.getStatus());
        }
        Assertions.assertEquals(BackupJobState.UPLOAD_SNAPSHOT, job.getState());

        // 4. upload snapshots
        AgentTaskQueue.clearAllTasks();
        job.run();
        Assertions.assertEquals(Status.OK, job.getStatus());
        Assertions.assertEquals(BackupJobState.UPLOADING, job.getState());
        Assertions.assertEquals(1, AgentTaskQueue.getTaskNum());

        AgentTask task = AgentTaskQueue.getTask(backendId, TTaskType.UPLOAD, id.get() - 1);
        Assertions.assertTrue(task instanceof UploadTask);
        UploadTask upTask = (UploadTask) task;

        Assertions.assertEquals(job.getJobId(), upTask.getJobId());
        Map<String, String> srcToDest = upTask.getSrcToDestPath();
        Assertions.assertEquals(1, srcToDest.size());
        String dest = srcToDest.get(snapshotPath + "/" + tabletId + "/" + 0);
        Assertions.assertNotNull(dest);

        // 5. uploading
        job.run();
        Assertions.assertEquals(Status.OK, job.getStatus());
        Assertions.assertEquals(BackupJobState.UPLOADING, job.getState());
        Map<Long, List<String>> tabletFileMap = Maps.newHashMap();
        TFinishTaskRequest request = new TFinishTaskRequest(tBackend, TTaskType.UPLOAD,
                    upTask.getSignature(), taskStatus);
        request.setTablet_files(tabletFileMap);

        Assertions.assertFalse(job.finishSnapshotUploadTask(upTask, request));
        List<String> tabletFiles = Lists.newArrayList();
        tabletFileMap.put(tabletId, tabletFiles);
        Assertions.assertFalse(job.finishSnapshotUploadTask(upTask, request));
        tabletFiles.add("1.dat.4f158689243a3d6030352fec3cfd3798");
        tabletFiles.add("wrong_files.4f158689243a3d6030352fec3cfd3798");
        Assertions.assertFalse(job.finishSnapshotUploadTask(upTask, request));
        tabletFiles.clear();
        tabletFiles.add("1.dat.4f158689243a3d6030352fec3cfd3798");
        tabletFiles.add("meta.4f158689243a3d6030352fec3cfd3798");
        Assertions.assertTrue(job.finishSnapshotUploadTask(upTask, request));
        job.run();
        Assertions.assertEquals(Status.OK, job.getStatus());
        Assertions.assertEquals(BackupJobState.SAVE_META, job.getState());

        // 6. save meta
        job.run();
        Assertions.assertEquals(Status.OK, job.getStatus());
        Assertions.assertEquals(BackupJobState.UPLOAD_INFO, job.getState());
        File metaInfo = new File(job.getLocalMetaInfoFilePath());
        Assertions.assertTrue(metaInfo.exists());
        File jobInfo = new File(job.getLocalJobInfoFilePath());
        Assertions.assertTrue(jobInfo.exists());

        BackupMeta restoreMetaInfo = null;
        BackupJobInfo restoreJobInfo = null;
        try {
            restoreMetaInfo = BackupMeta.fromFile(job.getLocalMetaInfoFilePath(), FeConstants.STARROCKS_META_VERSION);
            Assertions.assertEquals(2, restoreMetaInfo.getTables().size());

            {
                OlapTable olapTable = (OlapTable) restoreMetaInfo.getTable(tblId);
                Assertions.assertNotNull(olapTable);
                Assertions.assertNotNull(restoreMetaInfo.getTable(UnitTestUtil.TABLE_NAME));
                List<String> names = Lists.newArrayList(olapTable.getPartitionNames());
                Assertions.assertEquals(((OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                                        .getTable(db.getId(), tblId)).getSignature(BackupHandler.SIGNATURE_VERSION, names, true),
                            olapTable.getSignature(BackupHandler.SIGNATURE_VERSION, names, true));
            }
            {
                MaterializedView mv = (MaterializedView) restoreMetaInfo.getTable(tblId + 1);
                Assertions.assertNotNull(mv);
                Assertions.assertNotNull(restoreMetaInfo.getTable(UnitTestUtil.MATERIALIZED_VIEW_NAME));
                List<String> names = Lists.newArrayList(mv.getPartitionNames());
                Assertions.assertEquals(((OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                            .getTable(db.getId(), tblId + 1)).getSignature(BackupHandler.SIGNATURE_VERSION, names,
                            true), mv.getSignature(BackupHandler.SIGNATURE_VERSION, names, true));
            }

            restoreJobInfo = BackupJobInfo.fromFile(job.getLocalJobInfoFilePath());
            Assertions.assertEquals(UnitTestUtil.DB_NAME, restoreJobInfo.dbName);
            Assertions.assertEquals(job.getLabel(), restoreJobInfo.name);
            Assertions.assertEquals(2, restoreJobInfo.tables.size());

            // base table
            BackupJobInfo.BackupTableInfo baseTableBackupInfo = restoreJobInfo.getTableInfo(UnitTestUtil.TABLE_NAME);
            Assertions.assertTrue(baseTableBackupInfo != null);
            Table remoteBaseTable = backupMeta.getTable(UnitTestUtil.TABLE_NAME);
            Assertions.assertTrue(remoteBaseTable != null);

            // mv
            BackupJobInfo.BackupTableInfo mvBackupInfo = restoreJobInfo.getTableInfo(UnitTestUtil.TABLE_NAME);
            Assertions.assertTrue(mvBackupInfo != null);
            Table mvTable = backupMeta.getTable(UnitTestUtil.MATERIALIZED_VIEW_NAME);
            Assertions.assertTrue(mvTable != null);
            Assertions.assertTrue(mvTable instanceof MaterializedView);
            MaterializedView mv = (MaterializedView) mvTable;
            Assertions.assertTrue(mv != null);
            Assertions.assertTrue(!mv.isActive());
            Assertions.assertTrue(mv.getInactiveReason().contains(String.format("Set the materialized view %s inactive in backup",
                        UnitTestUtil.MATERIALIZED_VIEW_NAME)));
        } catch (IOException e) {
            Assertions.fail(e.getMessage());
        }

        Assertions.assertNull(job.getBackupMeta());
        Assertions.assertNull(job.getJobInfo());

        // 7. upload_info
        job.run();
        Assertions.assertEquals(Status.OK, job.getStatus());
        Assertions.assertEquals(BackupJobState.FINISHED, job.getState());

        if (job.getLocalJobDirPath() != null) {
            pathsNeedToBeDeleted.add(job.getLocalJobDirPath());
        }
    }

    @Test
    @Order(2)
    public void testRunAbnormal() {
        // 1.pending
        AgentTaskQueue.clearAllTasks();

        List<TableRef> tableRefs = Lists.newArrayList();
        tableRefs.add(new TableRef(new TableName(UnitTestUtil.DB_NAME, "unknown_tbl"), null));
        tableRefs.add(new TableRef(new TableName(UnitTestUtil.DB_NAME, "unknown_mv"), null));

        job = new BackupJob("mv_label_abnormal", dbId, UnitTestUtil.DB_NAME, tableRefs, 13600 * 1000,
                    globalStateMgr, repo.getId());
        job.run();
        Assertions.assertEquals(Status.ErrCode.NOT_FOUND, job.getStatus().getErrCode());
        Assertions.assertEquals(BackupJobState.CANCELLED, job.getState());

        if (job.getLocalJobDirPath() != null) {
            pathsNeedToBeDeleted.add(job.getLocalJobDirPath());
        }
    }
}
