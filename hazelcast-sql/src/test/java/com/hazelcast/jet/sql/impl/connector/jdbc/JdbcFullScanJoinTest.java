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

import java.sql.SQLException;

import static org.assertj.core.util.Lists.newArrayList;

public class JdbcFullScanJoinTest extends JdbcSqlTestSupport {

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

    private static String getInsertSQL(String tableName, int id, String workerName) {
        return String.format("INSERT INTO %s VALUES(%d, '%s')",
                tableName,
                id,
                workerName
        );
    }


    // This does not create equi join indices
    @Test
    public void selfJoin() throws SQLException {
        String tableName = randomTableName();
        createTable(tableName, "id INT PRIMARY KEY", "name VARCHAR(100)");

        String sql = getInsertSQL(tableName, 1, "Alice");
        executeJdbc(sql);

        sql = getInsertSQL(tableName, 2, "Barry");
        executeJdbc(sql);


        sql = getInsertSQL(tableName, 3, "Carol");
        executeJdbc(sql);

        execute(
                "CREATE MAPPING " + tableName + " ("
                + " id INT, "
                + " name VARCHAR "
                + ") "
                + "DATA CONNECTION " + TEST_DATABASE_REF
        );

        // Join on two columns
        assertRowsAnyOrder(
                "SELECT t1.name AS employee1, t2.name AS employee2 " +
                "FROM " + tableName + " t1 " +
                "JOIN " + tableName + " t2 " +
                "   ON LENGTH(t1.name) = LENGTH(t2.name) AND t1.id < t2.id",
                newArrayList(
                        new Row("Alice", "Barry"),
                        new Row("Alice", "Carol"),
                        new Row("Barry", "Carol")

                )
        );
    }

    // This does not create equi join indices
    @Test
    public void thetaJoin() throws SQLException {
        String otherTableName = randomTableName();
        createTable(otherTableName);


        String sql = getInsertSQL(otherTableName, 1, "name-1");
        executeJdbc(sql);

        sql = getInsertSQL(otherTableName, 2, "a");
        executeJdbc(sql);

        execute(
                "CREATE MAPPING " + otherTableName + " ("
                + " id INT, "
                + " name VARCHAR "
                + ") "
                + "DATA CONNECTION " + TEST_DATABASE_REF
        );

        String selectSql = "SELECT t1.name, t1.id, t2.id , t2.name " +
                           "FROM " + tableName + " t1 " +
                           "JOIN " + otherTableName + " t2 " +
                           "   ON t1.name <= t2.name";
        assertRowsAnyOrder(
                selectSql,
                newArrayList(
                        new Row("name-0", 0, 1, "name-1"),
                        new Row("name-1", 1, 1, "name-1")
                )
        );
    }

    // This does not create equi join indices
    @Test
    public void thetaJoinByPrimaryKey() throws SQLException {
        String otherTableName = randomTableName();
        createTable(otherTableName);


        String sql = getInsertSQL(otherTableName, 1, "name-1");
        executeJdbc(sql);

        sql = getInsertSQL(otherTableName, 2, "a");
        executeJdbc(sql);

        execute(
                "CREATE MAPPING " + otherTableName + " ("
                + " id INT, "
                + " name VARCHAR "
                + ") "
                + "DATA CONNECTION " + TEST_DATABASE_REF
        );

        String selectSql = "SELECT t1.name, t1.id, t2.id , t2.name " +
                           "FROM " + tableName + " t1 " +
                           "JOIN " + otherTableName + " t2 " +
                           "   ON t1.id < t2.id";
        assertRowsAnyOrder(
                selectSql,
                newArrayList(
                        new Row("name-0", 0, 1, "name-1"),
                        new Row("name-0", 0, 2, "a"),
                        new Row("name-1", 1, 2, "a")
                )
        );
    }
}
