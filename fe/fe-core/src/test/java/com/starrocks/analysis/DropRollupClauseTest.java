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

package com.starrocks.analysis;

import com.starrocks.common.AnalysisException;
import com.starrocks.sql.analyzer.AlterTableClauseAnalyzer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.DropRollupClause;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class DropRollupClauseTest {
    @Test
    public void testNormal() throws AnalysisException {
        DropRollupClause clause = new DropRollupClause("testRollup", null);
        AlterTableClauseAnalyzer analyzer = new AlterTableClauseAnalyzer(null);
        analyzer.analyze(null, clause);
        Assertions.assertEquals("DROP ROLLUP `testRollup`", clause.toString());
        Assertions.assertEquals("testRollup", clause.getRollupName());
    }

    @Test
    public void testNoRollup() {
        assertThrows(SemanticException.class, () -> {
            DropRollupClause clause = new DropRollupClause("", null);
            AlterTableClauseAnalyzer analyzer = new AlterTableClauseAnalyzer(null);
            analyzer.analyze(null, clause);
            Assertions.fail("No exception throws.");
        });
    }
}