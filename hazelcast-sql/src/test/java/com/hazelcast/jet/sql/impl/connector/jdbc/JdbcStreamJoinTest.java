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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class JdbcStreamJoinTest extends JdbcSqlTestSupport {

    @BeforeClass
    public static void beforeClass() {
        initialize(new H2DatabaseProvider());
    }

    private static String getWorkerName(int index) {
        return "myworker" + index;
    }

    private static int getSSN(int index) {
        return 207 + index;
    }

    private static String getInsertSQL(String tableName, int index) {
        return String.format("INSERT INTO %s VALUES(%d, '%s', %d)",
                tableName,
                index,
                getWorkerName(index),
                getSSN(index)
        );
    }

    // Left side is stream : joinInfo indices are not used
    @Test
    public void test_stream2BatchJoin() throws Exception {
        String tableName = randomTableName();
        createTable(tableName, "id INT PRIMARY KEY", "name VARCHAR(100)", "ssn INT DEFAULT 1");
        for (int index = 1; index < 3; index++) {
            String sql = getInsertSQL(tableName, index);
            executeJdbc(sql);
        }

        execute(
                "CREATE MAPPING " + tableName + " ("
                + " id INT, "
                + " name VARCHAR, "
                + " ssn INT "
                + ") "
                + "DATA CONNECTION " + TEST_DATABASE_REF
        );

        List<SqlRow> actualList = getRows("SELECT n.id, n.name, n.ssn , t.v FROM " +
                                          "TABLE(GENERATE_STREAM(2)) t " +
                                          "JOIN " + tableName + " n ON t.v = n.id LIMIT 2");

        List<Object> ssnList = actualList.stream()
                .map(sqlRow -> sqlRow.getObject("ssn"))
                .collect(Collectors.toList());

        assertThat(ssnList)
                .contains(208, 209);
    }


    // Left side is stream : joinInfo indices are not used
    @Test
    public void test_stream2BatchJoinJoinOnNonPrimaryKey() throws Exception {
        String tableName = randomTableName();
        createTable(tableName, "id INT PRIMARY KEY", "name VARCHAR(100)", "ssn INT DEFAULT 1");
        for (int index = 1; index < 3; index++) {
            String sql = getInsertSQL(tableName, index);
            executeJdbc(sql);
        }

        execute(
                "CREATE MAPPING " + tableName + " ("
                + " id INT, "
                + " name VARCHAR, "
                + " ssn INT "
                + ") "
                + "DATA CONNECTION " + TEST_DATABASE_REF
        );

        List<SqlRow> actualList = getRows("SELECT n.id, n.name, n.ssn , t.v FROM " +
                                          "TABLE(generate_stream(300)) t " +
                                          "JOIN " + tableName + " n ON t.v = n.ssn LIMIT 2");

        List<Object> ssnList = actualList.stream()
                .map(sqlRow -> sqlRow.getObject("ssn"))
                .collect(Collectors.toList());

        assertThat(ssnList)
                .contains(208, 209);
    }

    // Left side is stream : joinInfo indices are not used
    @Test
    public void test_stream2BatchLeftOuterJoin() throws Exception {
        String tableName = randomTableName();
        createTable(tableName, "id INT PRIMARY KEY", "name VARCHAR(100)", "ssn INT DEFAULT 1");
        for (int index = 1; index < 3; index++) {
            String sql = getInsertSQL(tableName, index);
            executeJdbc(sql);
        }

        execute(
                "CREATE MAPPING " + tableName + " ("
                + " id INT, "
                + " name VARCHAR, "
                + " ssn INT "
                + ") "
                + "DATA CONNECTION " + TEST_DATABASE_REF
        );

        List<SqlRow> actualList = getRows("SELECT n.id, n.name, n.ssn , t.v FROM " +
                                          "TABLE(GENERATE_STREAM(2)) t " +
                                          "LEFT OUTER JOIN " + tableName + " n ON t.v = n.id LIMIT 6");

        List<Object> ssnList = actualList.stream()
                .map(sqlRow -> sqlRow.getObject("ssn"))
                .collect(Collectors.toList());

        // id 0 : null
        // id 1 : 208 null
        // id 2 : 209 null
        assertThat(ssnList)
                .contains(null, null,
                        208, null,
                        209, null
                );
    }

    // Left side is stream : joinInfo indices are not used
    @Test
    public void test_stream2BatchLeftOuterJoinWithEmptyTable() throws Exception {
        String tableName = randomTableName();
        createTable(tableName, "id INT PRIMARY KEY", "name VARCHAR(100)", "ssn INT DEFAULT 1");

        execute(
                "CREATE MAPPING " + tableName + " ("
                + " id INT, "
                + " name VARCHAR, "
                + " ssn INT "
                + ") "
                + "DATA CONNECTION " + TEST_DATABASE_REF
        );

        List<SqlRow> actualList = getRows("SELECT n.id, n.name, n.ssn , t.v FROM " +
                                          "TABLE(GENERATE_STREAM(2)) t " +
                                          "LEFT OUTER JOIN " + tableName + " n ON t.v = n.id LIMIT 3");

        List<Object> ssnList = actualList.stream()
                .map(sqlRow -> sqlRow.getObject("ssn"))
                .collect(Collectors.toList());

        // All null because table is empty
        assertThat(ssnList)
                .contains(null, null,
                        null, null,
                        null, null
                );
    }

    private List<SqlRow> getRows(String sql) {
        List<SqlRow> actualList = new ArrayList<>();
        try (SqlResult sqlResult = sqlService.execute(sql)) {

            Iterator<SqlRow> iterator = sqlResult.iterator();
            iterator.forEachRemaining(actualList::add);
        }
        return actualList;
    }
}
