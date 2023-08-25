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
                        + "DATA CONNECTION " + TEST_DATABASE_REF
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
                        + "DATA CONNECTION " + TEST_DATABASE_REF
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
    public void deleteFromWithMultiplePKColumns() throws Exception {
        createTable(tableName, "id INT", "id2 INT", "name VARCHAR(10)", "PRIMARY KEY(id, id2)");
        executeJdbc("INSERT INTO " + tableName + " VALUES(0, 0, 'name-0')");
        executeJdbc("INSERT INTO " + tableName + " VALUES(1, 0, 'name-1')");
        executeJdbc("INSERT INTO " + tableName + " VALUES(0, 1, 'name-2')");
        execute(
                "CREATE MAPPING " + tableName + " ("
                        + " id INT, "
                        + " id2 INT, "
                        + " name VARCHAR"
                        + ") "
                        + "DATA CONNECTION " + TEST_DATABASE_REF
        );

        execute("DELETE FROM " + tableName + " WHERE id = 0 AND id2 = 1");

        assertJdbcRowsAnyOrder(tableName,
                new Row(0, 0, "name-0"),
                new Row(1, 0, "name-1")
        );
    }

    @Test
    public void deleteFromWithReverseColumnOrder() throws Exception {
        createTable(tableName, "name VARCHAR(10)", "id INT PRIMARY KEY");
        executeJdbc("INSERT INTO " + tableName + " VALUES('name-0', 0)");
        executeJdbc("INSERT INTO " + tableName + " VALUES('name-1', 1)");
        execute(
                "CREATE MAPPING " + tableName + " ("
                        + " name VARCHAR, "
                        + " id INT "
                        + ") "
                        + "DATA CONNECTION " + TEST_DATABASE_REF
        );

        execute("DELETE FROM " + tableName + " WHERE id = 0");

        assertJdbcRowsAnyOrder(tableName,
                new Row("name-1", 1)
        );
    }

    @Test
    public void deleteFromWithQuotedColumnInWhere() throws Exception {
        createTable(tableName, quote("person-id") + " INT PRIMARY KEY", "name VARCHAR(100)");
        insertItems(tableName, 1);

        execute(
                "CREATE MAPPING " + tableName + " DATA CONNECTION " + TEST_DATABASE_REF
        );

        execute("DELETE FROM " + tableName + " WHERE \"person-id\" = 0");
        assertJdbcRowsAnyOrder(tableName);
    }

}
