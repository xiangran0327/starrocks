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

package com.starrocks.qe;

import com.starrocks.common.FeConstants;
import com.starrocks.planner.OlapTableSink;
import com.starrocks.planner.RuntimeFilterDescription;
import com.starrocks.qe.scheduler.dag.ExecutionFragment;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.TPCDSPlanTest;
import com.starrocks.sql.plan.TPCDSPlanTestBase;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TPCDSCoordTest extends TPCDSPlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        TPCDSPlanTest.beforeClass();
    }

    @AfterAll
    public static void afterClass() {
    }

    @AfterEach
    public void tearDown() {
        ConnectContext ctx = starRocksAssert.getCtx();
        FeConstants.runningUnitTest = false;
        ctx.getSessionVariable().setEnablePipelineEngine(true);
    }

    @Test
    public void testQuery20() throws Exception {
        FeConstants.runningUnitTest = true;
        ConnectContext ctx = starRocksAssert.getCtx();
        ctx.setExecutionId(new TUniqueId(0x33, 0x0));
        ConnectContext.threadLocalInfo.set(ctx);
        ctx.getSessionVariable().setParallelExecInstanceNum(8);
        ctx.getSessionVariable().setEnablePipelineEngine(false);
        setTPCDSFactor(1);

        // make sure global runtime filter been push-downed to two fragments.
        String sql = "select * from (select a.inv_item_sk as x, b.inv_warehouse_sk " +
                "from inventory a join inventory b on a.inv_item_sk = b.inv_item_sk ) t1 " +
                "join [shuffle] item t0  on t0.i_item_sk = t1.x;";
        String plan = UtFrameUtils.getVerboseFragmentPlan(ctx, sql);
        String[] ss = plan.split("\\n");
        List<String> fragments = new ArrayList<>();
        String currentFragment = null;
        for (String s : ss) {
            if (s.indexOf("PLAN FRAGMENT") != -1) {
                currentFragment = s;
            }
            if (s.indexOf("filter_id = 1") != -1) {
                if (fragments.size() == 0 || !fragments.get(fragments.size() - 1).equals(currentFragment)) {
                    fragments.add(currentFragment);
                }
            }
        }
        // 1 fragment to generate filter(1)
        // 2 fragements to consumer filter(1)
        Assertions.assertEquals(3, fragments.size());

        ExecPlan execPlan = UtFrameUtils.getPlanAndFragment(ctx, sql).second;
        DefaultCoordinator coord = new DefaultCoordinator.Factory().createQueryScheduler(
                ctx, execPlan.getFragments(), execPlan.getScanNodes(), execPlan.getDescTbl().toThrift());
        coord.prepareExec();

        ExecutionFragment execFragment = coord.getExecutionDAG().getRootFragment();
        Assertions.assertEquals(15, execFragment.getRuntimeFilterParams().id_to_prober_params.get(1).size());
    }

    @Test
    public void testSubQueryExtractedFromQ5() throws Exception {
        FeConstants.runningUnitTest = true;
        ConnectContext ctx = starRocksAssert.getCtx();
        ctx.setExecutionId(new TUniqueId(0x33, 0x0));
        ConnectContext.threadLocalInfo.set(ctx);
        ctx.getSessionVariable().setParallelExecInstanceNum(8);
        ctx.getSessionVariable().setEnablePipelineEngine(true);
        setTPCDSFactor(1);

        // make sure global runtime filter been push-downed to two fragments.
        String sql = "SELECT COUNT(1)\n" +
                "from \n" +
                "(select wsr_web_site_sk, date_sk,sales_price,profit,return_amt,net_loss,d_date_sk, d_date\n" +
                "FROM (\n" +
                "    SELECT ws_web_site_sk AS wsr_web_site_sk, ws_sold_date_sk AS date_sk, " +
                "           ws_ext_sales_price AS sales_price, ws_net_profit AS profit, " +
                "           CAST(0 AS decimal(7, 2)) AS return_amt,\n" +
                "           CAST(0 AS decimal(7, 2)) AS net_loss\n" +
                "    FROM web_sales\n" +
                "    UNION ALL\n" +
                "    SELECT ws_web_site_sk AS wsr_web_site_sk, wr_returned_date_sk AS date_sk, " +
                "           CAST(0 AS decimal(7, 2)) AS sales_price, CAST(0 AS decimal(7, 2)) AS profit," +
                "           wr_return_amt AS return_amt,\n" +
                "           wr_net_loss AS net_loss\n" +
                "    FROM web_sales\n" +
                "        INNER JOIN web_returns\n" +
                "        ON wr_item_sk = ws_item_sk\n" +
                "            AND wr_order_number = ws_order_number\n" +
                ") salesreturns inner join[broadcast] date_dim on date_sk = d_date_sk) t " +
                "   inner join[broadcast] web_site on wsr_web_site_sk = web_site_sk\n" +
                "WHERE \n" +
                "    d_date BETWEEN CAST('2000-08-23' AS date) AND date_add(CAST('2000-08-23' AS date), 14)";
        String plan = UtFrameUtils.getVerboseFragmentPlan(ctx, sql);
        String[] ss = plan.split("\\n");
        List<String> filterLines = Stream.of(ss).filter(s -> s.contains("filter_id = 2")).collect(Collectors.toList());
        Assertions.assertTrue(filterLines.size() == 5);
        ExecPlan execPlan = UtFrameUtils.getPlanAndFragment(ctx, sql).second;
        DefaultCoordinator coord = new DefaultCoordinator.Factory().createQueryScheduler(
                ctx, execPlan.getFragments(), execPlan.getScanNodes(), execPlan.getDescTbl().toThrift());
        coord.prepareExec();

        int filterId = 2;
        boolean rfExists = false;
        for (ExecutionFragment execFragment : coord.getExecutionDAG().getFragmentsInPreorder()) {
            Map<Integer, RuntimeFilterDescription> buildRfFilters = execFragment.getPlanFragment().getBuildRuntimeFilters();
            if (buildRfFilters == null || !buildRfFilters.containsKey(filterId)) {
                continue;
            }
            RuntimeFilterDescription rf = buildRfFilters.get(filterId);
            Assertions.assertTrue(rf.isHasRemoteTargets() && rf.isBroadcastJoin());
            Assertions.assertFalse(rf.getBroadcastGRFDestinations().isEmpty());
            Assertions.assertTrue(rf.getBroadcastGRFDestinations().stream().anyMatch(d -> d.getFinstance_ids().size() >= 1));
            rfExists = true;
        }
        Assertions.assertTrue(rfExists);
    }

    @Test
    public void testOlapTableSinkAsGRFCoordinator() throws Exception {
        FeConstants.runningUnitTest = true;
        ConnectContext ctx = starRocksAssert.getCtx();
        ctx.setExecutionId(new TUniqueId(0x33, 0x0));
        ConnectContext.threadLocalInfo.set(ctx);
        ctx.getSessionVariable().setParallelExecInstanceNum(8);
        ctx.getSessionVariable().setEnablePipelineEngine(true);
        setTPCDSFactor(1);

        // make sure global runtime filter been push-downed to two fragments.
        String sql = "insert into item \n" +
                "select  item.*\n" +
                "from\n" +
                "     item inner join[shuffle] store_sales on store_sales.ss_item_sk = item.i_item_sk  \n" +
                "     inner join [shuffle] date_dim dt on dt.d_date_sk = store_sales.ss_sold_date_sk\n" +
                "where \n" +
                "   item.i_manufact_id = 128\n" +
                "   and dt.d_moy=11";
        String plan = UtFrameUtils.getVerboseFragmentPlan(ctx, sql);
        String[] ss = plan.split("\\n");
        List<String> filterLines = Stream.of(ss).filter(s -> s.contains("filter_id =")).collect(Collectors.toList());
        Assertions.assertFalse(filterLines.isEmpty());
        Assertions.assertTrue(filterLines.stream().anyMatch(ln -> ln.contains("remote = true")));
        ExecPlan execPlan = UtFrameUtils.getPlanAndFragment(ctx, sql).second;
        DefaultCoordinator coord = new DefaultCoordinator.Factory().createQueryScheduler(
                ctx, execPlan.getFragments(), execPlan.getScanNodes(), execPlan.getDescTbl().toThrift());
        coord.prepareExec();

        ExecutionFragment rootExecFragment = coord.getExecutionDAG().getFragmentsInPreorder().get(0);
        Assertions.assertTrue(rootExecFragment.getPlanFragment().getSink() instanceof OlapTableSink);
        Assertions.assertFalse(rootExecFragment.getRuntimeFilterParams().getRuntime_filter_builder_number().isEmpty());

        Set<TNetworkAddress> grfCoordinators =
                coord.getExecutionDAG().getFragmentsInPreorder().stream().flatMap(execFragment -> {
                    Map<Integer, RuntimeFilterDescription> buildRfFilters =
                            execFragment.getPlanFragment().getBuildRuntimeFilters();
                    if (buildRfFilters == null || buildRfFilters.isEmpty()) {
                        return Stream.empty();
                    } else {
                        return buildRfFilters.values().stream()
                                .filter(RuntimeFilterDescription::isHasRemoteTargets)
                                .flatMap(rf -> rf.toThrift().getRuntime_filter_merge_nodes().stream());
                    }
                }).collect(Collectors.toSet());

        Assertions.assertEquals(grfCoordinators.size(), 1);
        Assertions.assertTrue(
                grfCoordinators.contains(rootExecFragment.getInstances().get(0).getWorker().getBrpcAddress()));
    }
}
