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
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.util.Lists.newArrayList;

public class JdbcJoinTest extends JdbcSqlTestSupport {

    private static final int ITEM_COUNT = 5;

    private String tableName;

    @BeforeClass
    public static void beforeClass() {
        initialize(new H2DatabaseProvider());
    }

    @Before
    public void setUp() throws Exception {
        tableName = randomTableName();
        createTable(tableName);
        insertItems(tableName, ITEM_COUNT);

        execute(
                "CREATE MAPPING " + tableName + " ("
                + " id INT, "
                + " name VARCHAR "
                + ") "
                + "DATA CONNECTION " + TEST_DATABASE_REF
        );
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

    @Test
    public void test_stream2BatchJoinAsNestedLoopInnerJoin() throws Exception {
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

        SqlResult sqlResult = sqlService.execute("SELECT n.id, n.name, n.ssn , t.v FROM " +
                                                 "TABLE(GENERATE_STREAM(2)) t " +
                                                 "JOIN " + tableName + " n ON t.v = n.id LIMIT 2");

        Iterator<SqlRow> iterator = sqlResult.iterator();
        List<SqlRow> actualList = new ArrayList<>();
        iterator.forEachRemaining(actualList::add);

        List<Object> ssnList = actualList.stream()
                .map(sqlRow -> sqlRow.getObject("ssn"))
                .collect(Collectors.toList());

        assertThat(ssnList)
                .contains(208, 209);
    }

    @Test
    public void test_stream2BatchJoinAsNestedLoopLeftOuterJoin() throws Exception {
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

        SqlResult sqlResult = sqlService.execute("SELECT n.id, n.name, n.ssn , t.v FROM " +
                                                 "TABLE(GENERATE_STREAM(2)) t " +
                                                 "LEFT OUTER JOIN " + tableName + " n ON t.v = n.id LIMIT 6");

        Iterator<SqlRow> iterator = sqlResult.iterator();
        List<SqlRow> actualList = new ArrayList<>();
        iterator.forEachRemaining(actualList::add);

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

    @Test
    public void joinWithOtherJdbc() throws SQLException {
        String otherTableName = randomTableName();
        createTable(otherTableName);
        insertItems(otherTableName, ITEM_COUNT);

        execute(
                "CREATE MAPPING " + otherTableName + " ("
                + " id INT, "
                + " name VARCHAR "
                + ") "
                + "DATA CONNECTION " + TEST_DATABASE_REF
        );

        assertRowsAnyOrder(
                "SELECT t1.id, t2.name " +
                "FROM " + tableName + " t1 " +
                "JOIN " + otherTableName + " t2 " +
                "   ON t1.id = t2.id",
                newArrayList(
                        new Row(0, "name-0"),
                        new Row(1, "name-1"),
                        new Row(2, "name-2"),
                        new Row(3, "name-3"),
                        new Row(4, "name-4")
                )
        );
    }

    @Test
    public void joinWithOtherJdbcWhereClause() throws SQLException {
        String otherTableName = randomTableName();
        createTable(otherTableName);
        insertItems(otherTableName, ITEM_COUNT);

        execute(
                "CREATE MAPPING " + otherTableName + " ("
                + " id INT, "
                + " name VARCHAR "
                + ") "
                + "DATA CONNECTION " + TEST_DATABASE_REF
        );

        // Join on two columns
        assertRowsAnyOrder(
                "SELECT t1.id, t2.name " +
                "FROM " + tableName + " t1 " +
                "JOIN " + otherTableName + " t2 " +
                "   ON t1.id = t2.id AND t1.name = t2.name " +
                "WHERE t1.id > 0",
                newArrayList(
                        new Row(1, "name-1"),
                        new Row(2, "name-2"),
                        new Row(3, "name-3"),
                        new Row(4, "name-4")
                )
        );
    }

    @Test
    public void leftJoinWithOtherJdbc() throws SQLException {
        String otherTableName = randomTableName();
        createTable(otherTableName);
        insertItems(otherTableName, 3);

        execute(
                "CREATE MAPPING " + otherTableName + " ("
                + " id INT, "
                + " name VARCHAR "
                + ") "
                + "DATA CONNECTION " + TEST_DATABASE_REF
        );

        assertRowsAnyOrder(
                "SELECT t1.id, t2.name " +
                "FROM " + tableName + " t1 " +
                "LEFT JOIN " + otherTableName + " t2 " +
                "   ON t1.id = t2.id",
                newArrayList(
                        new Row(0, "name-0"),
                        new Row(1, "name-1"),
                        new Row(2, "name-2"),
                        new Row(3, null),
                        new Row(4, null)
                )
        );
    }

    @Test
    public void rightJoinWithOtherJdbc() throws SQLException {
        String otherTableName = randomTableName();
        createTable(otherTableName);
        insertItems(otherTableName, 7);

        execute(
                "CREATE MAPPING " + otherTableName + " ("
                + " id INT, "
                + " name VARCHAR "
                + ") "
                + "DATA CONNECTION " + TEST_DATABASE_REF
        );

        assertRowsAnyOrder(
                "SELECT t1.id, t2.name " +
                "FROM " + tableName + " t1 " +
                "RIGHT JOIN " + otherTableName + " t2 " +
                "   ON t1.id = t2.id",
                newArrayList(
                        new Row(0, "name-0"),
                        new Row(1, "name-1"),
                        new Row(2, "name-2"),
                        new Row(3, "name-3"),
                        new Row(4, "name-4"),
                        new Row(null, "name-5"),
                        new Row(null, "name-6")
                )
        );
    }

    @Test
    public void joinWithIMap() {
        String mapName = "my_map";
        execute(
                "CREATE MAPPING " + mapName + " ( " +
                "__key INT, " +
                "id INT, " +
                "name VARCHAR ) " +
                "TYPE IMap " +
                "OPTIONS (" +
                "    'keyFormat' = 'int'," +
                "    'valueFormat' = 'compact',\n" +
                "    'valueCompactTypeName' = 'person'" +
                ")"
        );

        execute("INSERT INTO " + mapName + "(__key, id, name)" +
                " SELECT v,v,'name-' || v FROM TABLE(generate_series(0,4))");

        assertRowsAnyOrder(
                "SELECT t1.id, t2.name " +
                "FROM " + tableName + " t1 " +
                "JOIN " + mapName + " t2 " +
                "   ON t1.id = t2.id",
                newArrayList(
                        new Row(0, "name-0"),
                        new Row(1, "name-1"),
                        new Row(2, "name-2"),
                        new Row(3, "name-3"),
                        new Row(4, "name-4")
                )
        );
    }

    @Test
    public void joinWithOtherJdbcNonDefaultSchema() throws SQLException {
        String schemaName = randomName();
        executeJdbc("CREATE SCHEMA " + schemaName);
        String fullyQualifiedTable = schemaName + "." + tableName;
        createTable(fullyQualifiedTable);
        insertItems(fullyQualifiedTable, ITEM_COUNT);
        String mappingName = randomTableName();
        createMapping(fullyQualifiedTable, mappingName);

        assertRowsAnyOrder(
                "SELECT t1.id, t2.name " +
                "FROM " + tableName + " t1 " +
                "JOIN \"" + mappingName + "\" t2 " +
                "   ON t1.id = t2.id",
                newArrayList(
                        new Row(0, "name-0"),
                        new Row(1, "name-1"),
                        new Row(2, "name-2"),
                        new Row(3, "name-3"),
                        new Row(4, "name-4")
                )
        );
    }
}
