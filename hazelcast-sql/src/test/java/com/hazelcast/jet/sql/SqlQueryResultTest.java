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

package com.hazelcast.jet.sql;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlExpectedResultType;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlStatement;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.emptyList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlQueryResultTest extends SqlTestSupport {

    private static final String MAP_NAME = "map";
    private static final String SQL_READ = "SELECT * FROM " + MAP_NAME;
    private static final String SQL_DELETE = "DELETE FROM " + MAP_NAME + " WHERE __key = 1";

    @BeforeClass
    public static void setUpClass() {
        initializeWithClient(1, null, null);
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
        HazelcastInstance member = instance();
        HazelcastInstance target = client ? client() : instance();

        createMapping(MAP_NAME, int.class, int.class);
        member.getMap(MAP_NAME).put(1, 1);

        // Check rows
        List<SqlRow> expectedRows = execute(SQL_READ);
        assertEquals(1, expectedRows.size());
        checkSuccess(target, SQL_READ, SqlExpectedResultType.ANY, expectedRows, -1);
        checkSuccess(target, SQL_READ, SqlExpectedResultType.ROWS, expectedRows, -1);
        checkFailure(target, SQL_READ, SqlExpectedResultType.UPDATE_COUNT);

        // Check update count
        // TODO: implement updateCount for DML and single key plans.
        checkSuccess(target, SQL_DELETE, SqlExpectedResultType.ANY, emptyList(), 0);
        checkFailure(target, SQL_DELETE, SqlExpectedResultType.ROWS);
        checkSuccess(target, SQL_DELETE, SqlExpectedResultType.UPDATE_COUNT, emptyList(), 0);
    }

    private List<SqlRow> execute(String query) {
        List<SqlRow> rows = new ArrayList<>();
        try (SqlResult result = instance().getSql().execute(query)) {
            for (SqlRow row : result) {
                rows.add(row);
            }
        }
        return rows;
    }

    private void checkSuccess(
            HazelcastInstance target,
            String sql,
            SqlExpectedResultType type,
            List<SqlRow> expectedRows,
            int expectedUpdateCount
    ) {
        SqlResult result = target.getSql().execute(new SqlStatement(sql).setExpectedResultType(type));
        assertEquals(expectedUpdateCount, result.updateCount());

        if (expectedUpdateCount >= 0) {
            return;
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
            result.iterator().forEachRemaining(row -> {
            });

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
