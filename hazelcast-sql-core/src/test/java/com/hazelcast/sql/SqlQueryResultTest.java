/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.sql;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.sql.impl.SqlServiceImpl;
import com.hazelcast.sql.impl.SqlTestSupport;
import com.hazelcast.sql.impl.plan.Plan;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlQueryResultTest extends SqlTestSupport {

    private static final String MAP_NAME = "map";
    private static final String SQL = "SELECT * FROM " + MAP_NAME;

    private final TestHazelcastFactory factory = new TestHazelcastFactory();

    @After
    public void after() {
        factory.shutdownAll();
    }

    @Test
    public void test_member() {
        check(false);
    }

    @Test
    public void test_client() {
        check(true);
    }

    private void check(boolean client) {
        HazelcastInstance member = factory.newHazelcastInstance();
        HazelcastInstance target = client ? factory.newHazelcastClient() : member;

        member.getMap(MAP_NAME).put(1, 1);

        // Install the ability to change the expected result of a plan.
        List<SqlRow> expectedRows = execute(member, SQL);

        SqlServiceImpl memberService = ((SqlServiceImpl) member.getSql());

        assertEquals(1, memberService.getPlanCache().size());

        TestPlan plan = new TestPlan((Plan) memberService.getPlanCache().getPlans().values().iterator().next());

        memberService.getPlanCache().put(plan.getPlanKey(), plan);

        // Check rows
        plan.setProducesRows(true);
        checkSuccess(target, SqlExpectedResultType.ANY, expectedRows);
        checkSuccess(target, SqlExpectedResultType.ROWS, expectedRows);
        checkFailure(target, SqlExpectedResultType.UPDATE_COUNT);

        // Check update count
        plan.setProducesRows(false);
        checkSuccess(target, SqlExpectedResultType.ANY, expectedRows);
        checkFailure(target, SqlExpectedResultType.ROWS);
        checkSuccess(target, SqlExpectedResultType.UPDATE_COUNT, expectedRows);
    }

    private void checkSuccess(HazelcastInstance target, SqlExpectedResultType type, List<SqlRow> expectedRows) {
        List<SqlRow> rows = executeStatement(target, new SqlStatement(SQL).setExpectedResultType(type));

        assertEquals(expectedRows.size(), rows.size());

        for (int i = 0; i < expectedRows.size(); i++) {
            SqlRow expectedRow = expectedRows.get(i);
            SqlRow row = rows.get(i);

            assertEquals(expectedRow.getMetadata(), row.getMetadata());

            for (int j = 0; j < expectedRow.getMetadata().getColumnCount(); j++) {
                Object expectedValue = expectedRow.getObject(j);
                Object value = row.getObject(j);

                assertEquals(expectedValue, value);
            }
        }
    }

    private void checkFailure(HazelcastInstance target, SqlExpectedResultType type) {
        assert type == SqlExpectedResultType.ROWS || type == SqlExpectedResultType.UPDATE_COUNT : type;

        try (SqlResult result = target.getSql().execute(new SqlStatement(SQL).setExpectedResultType(type))) {
            for (SqlRow ignore : result) {
                // No-op.
            }

            fail("Must fail");
        } catch (HazelcastSqlException e) {
            String message = e.getMessage();

            if (type == SqlExpectedResultType.ROWS) {
                assertEquals(message, "The statement doesn't produce rows");
            } else {
                assertEquals(message, "The statement doesn't produce update count");
            }
        }
    }

    private static class TestPlan extends Plan {

        private boolean producesRows;

        private TestPlan(Plan plan) {
            super(
                plan.getPartitionMap(),
                plan.getFragments(),
                plan.getFragmentMappings(),
                plan.getOutboundEdgeMap(),
                plan.getInboundEdgeMap(),
                plan.getInboundEdgeMemberCountMap(),
                plan.getRowMetadata(),
                plan.getParameterMetadata(),
                plan.getPlanKey(),
                plan.getObjectKeys(),
                plan.getPermissions()
            );
        }

        @Override
        public boolean producesRows() {
            return producesRows;
        }

        public void setProducesRows(boolean producesRows) {
            this.producesRows = producesRows;
        }
    }
}
