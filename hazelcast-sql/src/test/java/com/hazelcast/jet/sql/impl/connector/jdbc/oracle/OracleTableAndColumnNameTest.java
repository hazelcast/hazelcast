/*
 * Copyright 2025 Hazelcast Inc.
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
package com.hazelcast.jet.sql.impl.connector.jdbc.oracle;

import com.hazelcast.jet.sql.impl.connector.jdbc.JdbcSqlTestSupport;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.test.jdbc.OracleDatabaseProviderFactory;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.SQLException;

import static org.assertj.core.util.Lists.newArrayList;

public class OracleTableAndColumnNameTest extends JdbcSqlTestSupport {

    private String tableName;
    @BeforeClass
    public static void beforeClass() {
        initialize(OracleDatabaseProviderFactory.createTestDatabaseProvider());
    }

    @Test
    public void tableNameWithoutQuotes() throws SQLException {
        tableName = "MyTable0";
        createTableNoQuote(tableName);
        Exception exception = assertThrows(HazelcastSqlException.class, () -> createMapping(tableName));
        assertContains(exception.getMessage(), "Could not execute readDbFields for table");
        //Oracle Converts MyTable0 to MYTABLE0 internally
        createMapping("MYTABLE0");
        execute("INSERT INTO MYTABLE0 VALUES (0, 'name-0')");

        assertJdbcRowsAnyOrder("MYTABLE0",
                newArrayList(Integer.class, String.class),
                new Row(0, "name-0"));
    }

    @Test
    public void tableNameWithQuotes() throws SQLException {
        tableName = "MyTable1";
        //creates a table with quoted table name
        createTable(tableName);
        createMapping(tableName);
        execute("INSERT INTO " + tableName + " VALUES (0, 'name-0')");

        assertJdbcRowsAnyOrder(tableName,
                newArrayList(Integer.class, String.class),
                new Row(0, "name-0"));
    }

    @Test
    public void tableNameUpperCase() throws SQLException {
        tableName = "MYTABLE2";
        createTableNoQuote(tableName);
        createMapping(tableName);
        execute("INSERT INTO " + tableName + " VALUES (0, 'name-0')");

        assertJdbcRowsAnyOrder(tableName,
                newArrayList(Integer.class, String.class),
                new Row(0, "name-0"));
    }

    @Test
    public void columnNameWithoutQuotes() throws SQLException {
        tableName = "MyTable3";
        createTableNoQuote(databaseProvider.quote(tableName), "id INT PRIMARY KEY, name VARCHAR(100)");
        Exception exception = assertThrows(HazelcastSqlException.class,
                () -> createMapping(tableName));
        assertContains(exception.getMessage(), "Could not resolve field with name");
    }

    @Test
    public void columnNameUpperCase() throws SQLException {
        tableName = "MyTable4";
        createTableNoQuote(databaseProvider.quote(tableName), "ID INT PRIMARY KEY, NAME VARCHAR(100)");
        execute(
                "CREATE MAPPING " + tableName + " ("
                        + " ID INT, "
                        + " NAME VARCHAR "
                        + ") "
                        + "DATA CONNECTION " + TEST_DATABASE_REF
        );
        execute("INSERT INTO " + tableName + " VALUES (0, 'name-0')");

        assertJdbcRowsAnyOrder(tableName,
                newArrayList(Integer.class, String.class),
                new Row(0, "name-0"));
    }

}
