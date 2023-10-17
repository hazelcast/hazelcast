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

public class JdbcInnerEquiJoinTest extends JdbcSqlTestSupport {

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

    private static String getInsertSQL(String tableName, int id, String workerName) {
        return String.format("INSERT INTO %s VALUES(%d, '%s')",
                tableName,
                id,
                workerName
        );
    }


    // Left side is batch : joinInfo indices are used
    @Test
    public void joinWithTableValuedFunction() throws Exception {
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
                                          "TABLE(generate_series(207,210)) t " +
                                          "JOIN " + tableName + " n ON t.v = n.ssn LIMIT 2");

        List<Object> ssnList = actualList.stream()
                .map(sqlRow -> sqlRow.getObject("ssn"))
                .collect(Collectors.toList());

        assertThat(ssnList)
                .contains(208, 209);
    }


    // Left side is batch : joinInfo indices are used
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

    private String getDummyTable1Sql(String tableName, int id, String name) {
        return String.format("INSERT INTO %s (id,name) VALUES(%d,%s)", tableName, id, name);
    }

    // Left side is batch : joinInfo indices are used
    @Test
    public void joinWithOtherJdbc_Right_Side_Has_Different_Mapping_Order() throws SQLException {
        String otherTableName1 = randomTableName();
        createTable(otherTableName1, "id INT PRIMARY KEY", "name VARCHAR(10)",
                "dummy1 VARCHAR(10) DEFAULT 'dummy1'", "dummy2 VARCHAR(10) DEFAULT 'dummy2'");

        String sql = getDummyTable1Sql(otherTableName1, 1, "'Alice'");
        executeJdbc(sql);

        execute(
                "CREATE MAPPING " + otherTableName1 + " ("
                + " id INT, "
                + " dummy1 VARCHAR ,"
                + " dummy2 VARCHAR ,"
                + " name VARCHAR "
                + ") "
                + "DATA CONNECTION " + TEST_DATABASE_REF
        );

        sql = getDummyTable1Sql(otherTableName1, 2, "'Bob'");
        executeJdbc(sql);

        String otherTableName2 = randomTableName();
        createTable(otherTableName2, "id INT PRIMARY KEY", "dummy1 VARCHAR(10) DEFAULT 'dummy1'",
                "dummy2 VARCHAR(10) DEFAULT 'dummy2'", "name VARCHAR(10)");
        sql = getDummyTable1Sql(otherTableName2, 1, "'Alice'");
        executeJdbc(sql);

        execute(
                "CREATE MAPPING " + otherTableName2 + " ("
                + " name VARCHAR ,"
                + " id INT "
                + ") "
                + "DATA CONNECTION " + TEST_DATABASE_REF
        );

        assertRowsAnyOrder(
                "SELECT t1.dummy1, t1.dummy2, t1.id, t2.name " +
                "FROM " + otherTableName1 + " t1 " +
                "JOIN " + otherTableName2 + " t2 " +
                "   ON t1.id = t2.id AND t1.name = t2.name",
                newArrayList(
                        new Row("dummy1", "dummy2", 1, "Alice")
                )
        );
    }

    // Left side is batch : joinInfo indices are used
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

    // Left side is batch : joinInfo indices are used
    @Test
    public void joinWithOtherJdbcWhereClauseOnRightSideColumn() throws SQLException {
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
                "WHERE t2.id > 0",
                newArrayList(
                        new Row(1, "name-1"),
                        new Row(2, "name-2"),
                        new Row(3, "name-3"),
                        new Row(4, "name-4")
                )
        );
    }


    // Left side is batch : joinInfo indices are used
    @Test
    public void joinWithOtherJdbc_right_Side_Has_Less_Rows() throws SQLException {
        String otherTableName = randomTableName();
        createTable(otherTableName);


        String sql = getInsertSQL(otherTableName, 1, "name-1");
        executeJdbc(sql);
        sql = getInsertSQL(otherTableName, 3, "name-1");
        executeJdbc(sql);


        execute(
                "CREATE MAPPING " + otherTableName + " ("
                + " id INT, "
                + " name VARCHAR "
                + ") "
                + "DATA CONNECTION " + TEST_DATABASE_REF
        );

        assertRowsAnyOrder(
                "SELECT t1.name, t1.id, t2.id " +
                "FROM " + tableName + " t1 " +
                "JOIN " + otherTableName + " t2 " +
                "   ON t1.name = t2.name",
                newArrayList(
                        new Row("name-1", 1, 1),
                        new Row("name-1", 1, 3)
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

    private List<SqlRow> getRows(String sql) {
        List<SqlRow> actualList = new ArrayList<>();
        try (SqlResult sqlResult = sqlService.execute(sql)) {

            Iterator<SqlRow> iterator = sqlResult.iterator();
            iterator.forEachRemaining(actualList::add);
        }
        return actualList;
    }
}
