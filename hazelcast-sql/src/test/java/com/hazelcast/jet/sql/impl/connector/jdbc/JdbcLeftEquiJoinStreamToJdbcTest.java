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

import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.test.jdbc.H2DatabaseProvider;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class JdbcLeftEquiJoinStreamToJdbcTest extends JdbcSqlTestSupport {

    @BeforeClass
    public static void beforeClass() {
        initialize(new H2DatabaseProvider());
    }


    // This test will call JdbcSqlConnector#nestedLoopReader
    // joinInfo indices are used
    @Test
    public void joinWithTableValuedFunction_small_table_on_right() throws Exception {
        String tableName = "table1";
        createTable(tableName, "id INT PRIMARY KEY", "name VARCHAR(100)", "ssn INT DEFAULT 1");
        // SSN from : 208 - 210
        addWorkers(tableName, 3);

        execute(
                "CREATE MAPPING " + tableName + " ("
                + " id INT, "
                + " name VARCHAR, "
                + " ssn INT "
                + ") "
                + "DATA CONNECTION " + TEST_DATABASE_REF
        );

        String sql = "SELECT n.id, n.name, n.ssn , t.v FROM " +
                     "TABLE(generate_series(207,211)) t " +
                     "LEFT JOIN " + tableName + " n ON t.v = n.ssn ORDER BY n.ssn, t.v";
        List<Row> actualList = getRows(sql);


        assertThat(actualList).containsExactly(
                new Row(null, null, null, 207),
                new Row(null, null, null, 211),
                new Row(1, "myworker1", 208, 208),
                new Row(2, "myworker2", 209, 209),
                new Row(3, "myworker3", 210, 210)
                );
    }

    // This test will call JdbcSqlConnector#fullScanReader
    @Test
    public void joinWithTableValuedFunction_small_table_on_left() throws Exception {
        String tableName = "table2";
        createTable(tableName, "id INT PRIMARY KEY", "name VARCHAR(100)", "ssn INT DEFAULT 1");
        // SSN from : 208 - 210
        addWorkers(tableName, 3);

        execute(
                "CREATE MAPPING " + tableName + " ("
                + " id INT, "
                + " name VARCHAR, "
                + " ssn INT "
                + ") "
                + "DATA CONNECTION " + TEST_DATABASE_REF
        );

        String sql = "SELECT n.id, n.name, n.ssn , t.v FROM " + tableName + " n " +
                     "LEFT JOIN TABLE(generate_series(207,300)) t ON t.v = n.ssn ORDER BY n.ssn";
        List<Row> actualList = getRows(sql);


        assertThat(actualList).containsExactly(
                new Row(1, "myworker1", 208, 208),
                new Row(2, "myworker2", 209, 209),
                new Row(3, "myworker3", 210, 210)
        );
    }

    private List<Row> getRows(String sql) {
        List<Row> actualList = new ArrayList<>();
        try (SqlResult sqlResult = sqlService.execute(sql)) {

            Iterator<SqlRow> iterator = sqlResult.iterator();
            iterator.forEachRemaining(row -> actualList.add(new Row(row)));
        }
        return actualList;
    }

    private static void addWorkers(String tableName, int count) throws SQLException {
        for (int index = 1; index <= count; index++) {
            // (1, 'myworker1', 208),
            // (2, 'myworker2', 209)
            // ...
            // (4, 'myworker4', 211)
            String sql = getInsertWorkerSQL(tableName, index);
            executeJdbc(sql);
        }
    }

    private static String getWorkerName(int index) {
        return "myworker" + index;
    }

    private static int getSSN(int index) {
        return 207 + index;
    }

    private static String getInsertWorkerSQL(String tableName, int index) {
        return String.format("INSERT INTO %s VALUES(%d, '%s', %d)",
                tableName,
                index,
                getWorkerName(index),
                getSSN(index)
        );
    }
}
