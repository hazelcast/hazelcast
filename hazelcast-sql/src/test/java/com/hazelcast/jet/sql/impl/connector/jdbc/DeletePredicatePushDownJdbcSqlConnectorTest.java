/*
 * Copyright 2023 Hazelcast Inc.
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

package com.hazelcast.jet.sql.impl.connector.jdbc;

import com.hazelcast.test.jdbc.H2DatabaseProvider;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.assertj.core.util.Lists.newArrayList;

public class DeletePredicatePushDownJdbcSqlConnectorTest extends JdbcSqlTestSupport {

    private static final String JSON = "{\"value\":42}";
    private String tableName;

    @BeforeClass
    public static void beforeClass() {
        initialize(new H2DatabaseProvider());
    }

    @Before
    public void setUp() throws Exception {
        tableName = randomTableName();

        createTable(tableName, "id INT PRIMARY KEY", "name VARCHAR(100)", "age INT", "data VARCHAR(100)");
        executeJdbc("INSERT INTO " + quote(tableName) + " VALUES(0, 'name-0', 0, '{\"value\":42}')");
        executeJdbc("INSERT INTO " + quote(tableName) + " VALUES(1, 'name-1', 1, '{\"value\":42}')");
        execute(
                "CREATE MAPPING " + tableName + " ("
                        + " id INT, "
                        + " name VARCHAR, "
                        + " age INT, "
                        + " data VARCHAR "
                        + ") "
                        + "DATA CONNECTION " + TEST_DATABASE_REF
        );
    }

    @Test
    public void noParameterNoPredicate() throws Exception {
        execute("DELETE FROM " + tableName);

        assertJdbcRowsAnyOrder(tableName); // no rows
    }

    @Test
    public void noParameterPredicateCanPushDown() throws Exception {
        execute("DELETE FROM " + tableName + " WHERE age = 0");

        assertJdbcRowsAnyOrder(tableName,
                newArrayList(Integer.class, String.class, Integer.class, String.class),
                new Row(1, "name-1", 1, JSON)
        );
    }

    @Test
    public void noParameterPredicateCanNotPushDown() throws Exception {
        execute("DELETE FROM " + tableName + " WHERE age = 0 AND JSON_QUERY(data, '$.value') = '42'");

        assertJdbcRowsAnyOrder(tableName,
                newArrayList(Integer.class, String.class, Integer.class, String.class),
                new Row(1, "name-1", 1, JSON)
        );
    }

    @Test
    public void parameterInWhereClausePredicateCanPushDown() throws Exception {
        execute("DELETE FROM " + tableName + " WHERE age = ?", 0);

        assertJdbcRowsAnyOrder(tableName,
                newArrayList(Integer.class, String.class, Integer.class, String.class),
                new Row(1, "name-1", 1, JSON)
        );
    }

    @Test
    public void parameterInWhereClausePredicateCanPushDownCastColumn() throws Exception {
        execute("DELETE FROM " + tableName + " WHERE CAST(age as VARCHAR(100)) = ?", "0");

        assertJdbcRowsAnyOrder(tableName,
                newArrayList(Integer.class, String.class, Integer.class, String.class),
                new Row(1, "name-1", 1, JSON)
        );
    }

    @Test
    public void parameterInWhereClausePredicateCanPushDownCastParameter() throws Exception {
        execute("DELETE FROM " + tableName + " WHERE age = CAST(? as INTEGER)", "0");

        assertJdbcRowsAnyOrder(tableName,
                newArrayList(Integer.class, String.class, Integer.class, String.class),
                new Row(1, "name-1", 1, JSON)
        );
    }

    @Test
    public void parameterInWhereClausePredicateCanNotPushDown() throws Exception {
        execute(
                "DELETE FROM " + tableName + " WHERE age = ? AND JSON_QUERY(data, '$.value') = '42'",
                0
        );

        assertJdbcRowsAnyOrder(tableName,
                newArrayList(Integer.class, String.class, Integer.class, String.class),
                new Row(1, "name-1", 1, JSON)
        );
    }

    @Test
    public void parameterInWhereClauseAndPredicatePredicateCanNotPushDown() throws Exception {
        execute(
                "DELETE FROM " + tableName + " WHERE age = ? AND JSON_QUERY(data, '$.value') = ?",
                0, "42"
        );

        assertJdbcRowsAnyOrder(tableName,
                newArrayList(Integer.class, String.class, Integer.class, String.class),
                new Row(1, "name-1", 1, JSON)
        );
    }

}
