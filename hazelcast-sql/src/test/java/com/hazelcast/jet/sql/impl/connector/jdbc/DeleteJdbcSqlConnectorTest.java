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

package com.hazelcast.jet.sql.impl.connector.jdbc;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.hazelcast.jet.sql.impl.connector.jdbc.JdbcSqlConnector.OPTION_JDBC_URL;

public class DeleteJdbcSqlConnectorTest extends JdbcSqlTestSupport {

    private String tableName;

    @BeforeClass
    public static void beforeClass() {
        initialize(new H2DatabaseProvider());
    }

    @Before
    public void setUp() throws Exception {
        tableName = randomTableName();
    }

    @Test
    public void deleteFromTable() throws Exception {
        createTable(tableName);
        insertItems(tableName, 2);
        createMapping(tableName);

        execute("DELETE FROM " + tableName);

        assertJdbcRowsAnyOrder(tableName);
    }

    @Test
    public void deleteFromTableWhereId() throws Exception {
        createTable(tableName);
        insertItems(tableName, 2);
        createMapping(tableName);

        execute("DELETE FROM " + tableName + " WHERE id = 0");

        assertJdbcRowsAnyOrder(tableName, new Row(1, "name-1"));
    }

    @Test
    public void deleteFromTableWhereIdColumnWithExternalName() throws Exception {
        createTable(tableName);
        insertItems(tableName, 2);
        execute(
                "CREATE MAPPING " + tableName + " ("
                        + " person_id INT EXTERNAL NAME id, "
                        + " name VARCHAR "
                        + ") "
                        + "TYPE " + JdbcSqlConnector.TYPE_NAME + ' '
                        + "OPTIONS ( "
                        + " '" + OPTION_JDBC_URL + "'='" + dbConnectionUrl + "'"
                        + ")"
        );

        execute("DELETE FROM " + tableName + " WHERE person_id = 0");

        assertJdbcRowsAnyOrder(tableName, new Row(1, "name-1"));
    }

    @Test
    public void deleteFromTableWhereOnNonPKColumn() throws Exception {
        createTable(tableName);
        insertItems(tableName, 2);
        createMapping(tableName);

        execute("DELETE FROM " + tableName + " WHERE name = 'name-0'");

        assertJdbcRowsAnyOrder(tableName, new Row(1, "name-1"));
    }
    @Test
    public void deleteFromTableWhereOnNonPKColumnWithExternalNme() throws Exception {
        createTable(tableName);
        insertItems(tableName, 2);
        execute(
                "CREATE MAPPING " + tableName + " ("
                        + " id INT, "
                        + " fullName VARCHAR EXTERNAL NAME name "
                        + ") "
                        + "TYPE " + JdbcSqlConnector.TYPE_NAME + ' '
                        + "OPTIONS ( "
                        + " '" + OPTION_JDBC_URL + "'='" + dbConnectionUrl + "'"
                        + ")"
        );

        execute("DELETE FROM " + tableName + " WHERE fullName = 'name-0'");

        assertJdbcRowsAnyOrder(tableName, new Row(1, "name-1"));
    }

    @Test
    public void deleteFromTableUsingMappingName() throws Exception {
        createTable(tableName);
        insertItems(tableName, 2);

        String mappingName = "mapping_" + randomName();
        createMapping(tableName, mappingName);

        execute("DELETE FROM " + mappingName);

        assertJdbcRowsAnyOrder(tableName);
    }

    @Test
    public void updateTableWithMultiplePKColumns() throws Exception {
        createTable(tableName, "id INT", "id2 INT", "name VARCHAR(10)", "PRIMARY KEY(id, id2)");
        executeJdbc("INSERT INTO " + tableName + " VALUES(0, 0, 'name-0')");
        executeJdbc("INSERT INTO " + tableName + " VALUES(1, 0, 'name-1')");
        execute(
                "CREATE MAPPING " + tableName + " ("
                        + " id INT, "
                        + " id2 INT, "
                        + " name VARCHAR"
                        + ") "
                        + "TYPE " + JdbcSqlConnector.TYPE_NAME + ' '
                        + "OPTIONS ( "
                        + " '" + OPTION_JDBC_URL + "'='" + dbConnectionUrl + "'"
                        + ")"
        );

        execute("DELETE FROM " + tableName + " WHERE id = 0 AND id2 = 0");

        assertJdbcRowsAnyOrder(tableName,
                new Row(1, 0, "name-1")
        );
    }
}
