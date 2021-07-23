/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.sql;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.sql.impl.SqlTestSupport;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlQueryResultTest extends SqlTestSupport {

    private static final String MAP_NAME = "map";
    private static final String SQL_READ = "SELECT * FROM " + MAP_NAME;
    private static final String SQL_UPDATE = "DELETE FROM " + MAP_NAME + " WHERE __key = 1";

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

        // Check rows
        List<SqlRow> expectedRows = execute(member, SQL_READ);
        assertEquals(1, expectedRows.size());
        checkSuccess(target, SQL_READ, SqlExpectedResultType.ROWS, expectedRows);
        checkSuccess(target, SQL_READ, SqlExpectedResultType.ANY, expectedRows);
        checkFailure(target, SQL_READ, SqlExpectedResultType.UPDATE_COUNT);

        // Check update count
        checkFailure(target, SQL_UPDATE, SqlExpectedResultType.ROWS);
        // TODO: implement updateCount for DML and single key plans.
        checkSuccess(target, SQL_UPDATE, SqlExpectedResultType.ANY, expectedRows);
        checkSuccess(target, SQL_UPDATE, SqlExpectedResultType.UPDATE_COUNT, expectedRows);
    }

    private void checkSuccess(HazelcastInstance target, String sql, SqlExpectedResultType type, List<SqlRow> expectedRows) {
        SqlResult result = target.getSql().execute(new SqlStatement(sql).setExpectedResultType(type));
        if (type == SqlExpectedResultType.UPDATE_COUNT) {
            // TODO: update after updateCount for DML and single key plans implementation.
            assertGreaterOrEquals("updateCount", result.updateCount(), 0);
            return;
        }

        if (type == SqlExpectedResultType.ANY) {
            if (sql.equals(SQL_UPDATE)) {
                assertGreaterOrEquals("updateCount", result.updateCount(), 0);
                return;
            }
        }

        List<SqlRow> rows = new ArrayList<>();
        for (SqlRow row : result) {
            rows.add(row);
        }
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

    private void checkFailure(HazelcastInstance target, String sql, SqlExpectedResultType type) {
        assert type == SqlExpectedResultType.ROWS || type == SqlExpectedResultType.UPDATE_COUNT : type;

        try (SqlResult result = target.getSql().execute(new SqlStatement(sql).setExpectedResultType(type))) {
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
}
