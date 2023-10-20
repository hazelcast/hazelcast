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

public class UpdateJdbcSqlConnectorTest extends JdbcSqlTestSupport {

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
    public void updateTable() throws Exception {
        createTable(tableName);
        insertItems(tableName, 2);
        execute(
                "CREATE MAPPING " + tableName + " ("
                        + " id INT, "
                        + " name VARCHAR "
                        + ") "
                        + "DATA CONNECTION " + TEST_DATABASE_REF
        );

        execute("UPDATE " + tableName + " SET name = 'updated'");

        assertJdbcRowsAnyOrder(tableName,
                newArrayList(Integer.class, String.class),
                new Row(0, "updated"),
                new Row(1, "updated")
        );
    }

    @Test
    public void updateTableWhereId() throws Exception {
        createTable(tableName);
        insertItems(tableName, 2);
        execute(
                "CREATE MAPPING " + tableName + " ("
                        + " id INT, "
                        + " name VARCHAR "
                        + ") "
                        + "DATA CONNECTION " + TEST_DATABASE_REF
        );

        execute("UPDATE " + tableName + " SET name = 'updated' WHERE id=0");

        assertJdbcRowsAnyOrder(tableName,
                newArrayList(Integer.class, String.class),
                new Row(0, "updated"),
                new Row(1, "name-1")
        );
    }

    @Test
    public void updateTableWhereIdUsingQueryParameter() throws Exception {
        createTable(tableName);
        insertItems(tableName, 2);
        execute(
                "CREATE MAPPING " + tableName + " ("
                        + " id INT, "
                        + " name VARCHAR "
                        + ") "
                        + "DATA CONNECTION " + TEST_DATABASE_REF
        );

        execute("UPDATE " + tableName + " SET name = 'updated' WHERE id = ?", 0);

        assertJdbcRowsAnyOrder(tableName,
                newArrayList(Integer.class, String.class),
                new Row(0, "updated"),
                new Row(1, "name-1")
        );
    }

    @Test
    public void updateTableWhereOnNonPKColumn() throws Exception {
        createTable(tableName);
        insertItems(tableName, 2);
        execute(
                "CREATE MAPPING " + tableName + " ("
                        + " id INT, "
                        + " name VARCHAR "
                        + ") "
                        + "DATA CONNECTION " + TEST_DATABASE_REF
        );

        execute("UPDATE " + tableName + " SET name = 'updated' WHERE name='name-0'");

        assertJdbcRowsAnyOrder(tableName,
                newArrayList(Integer.class, String.class),
                new Row(0, "updated"),
                new Row(1, "name-1")
        );
    }

    @Test
    public void updateTableWhereColumnWithExternalName() throws Exception {
        createTable(tableName);
        insertItems(tableName, 2);
        execute(
                "CREATE MAPPING " + tableName + " ("
                        + " person_id INT EXTERNAL NAME id, "
                        + " name VARCHAR"
                        + ") "
                        + "DATA CONNECTION " + TEST_DATABASE_REF
        );

        execute("UPDATE " + tableName + " SET name = 'updated' WHERE person_id = 0");

        assertJdbcRowsAnyOrder(tableName,
                newArrayList(Integer.class, String.class),
                new Row(0, "updated"),
                new Row(1, "name-1")
        );
    }

    @Test
    public void updateTableSetColumnWithExternalName() throws Exception {
        createTable(tableName);
        insertItems(tableName, 2);
        execute(
                "CREATE MAPPING " + tableName + " ("
                        + " id INT, "
                        + " fullName VARCHAR EXTERNAL NAME name"
                        + ") "
                        + "DATA CONNECTION " + TEST_DATABASE_REF
        );

        execute("UPDATE " + tableName + " SET fullName = 'updated' WHERE id = 0");

        assertJdbcRowsAnyOrder(tableName,
                newArrayList(Integer.class, String.class),
                new Row(0, "updated"),
                new Row(1, "name-1")
        );
    }

    @Test
    public void updateTableSetUsingExpressionWithTableColumn() throws Exception {
        createTable(tableName);
        insertItems(tableName, 2);
        execute(
                "CREATE MAPPING " + tableName + " ("
                        + " id INT, "
                        + " name VARCHAR "
                        + ") "
                        + "DATA CONNECTION " + TEST_DATABASE_REF
        );

        execute("UPDATE " + tableName + " SET name = 'updated-'||id");

        assertJdbcRowsAnyOrder(tableName,
                newArrayList(Integer.class, String.class),
                new Row(0, "updated-0"),
                new Row(1, "updated-1")
        );
    }

    @Test
    public void updateTableSetUsingExpressionWithTableColumnNoPushDown() throws Exception {
        createTable(tableName, "id INT PRIMARY KEY", "name VARCHAR(10)", "data VARCHAR(100)");
        executeJdbcWithQuotes("INSERT INTO " + tableName + " VALUES(0, 'name-0', '{\"value\":0}')", tableName);
        executeJdbcWithQuotes("INSERT INTO " + tableName + " VALUES(1, 'name-1', '{\"value\":1}')", tableName);
        executeJdbcWithQuotes("INSERT INTO " + tableName + " VALUES(2, 'name-2', '{\"value\":2}')", tableName);
        execute(
                "CREATE MAPPING " + tableName + " ("
                        + " id INT, "
                        + " name VARCHAR, "
                        + " data VARCHAR"
                        + ") "
                        + "DATA CONNECTION " + TEST_DATABASE_REF
        );

        execute("UPDATE " + tableName + " SET name = 'updated-'||id WHERE JSON_QUERY(data, '$.value') = '2'");

        assertJdbcQueryRowsAnyOrder("SELECT " + quote("id") + ", " + quote("name") + " FROM " + quote(tableName),
                newArrayList(Integer.class, String.class),
                new Row(0, "name-0"),
                new Row(1, "name-1"),
                new Row(2, "updated-2")
        );
    }

    @Test
    public void updateTableSetUsingQueryParameter() throws Exception {
        createTable(tableName);
        insertItems(tableName, 1);
        execute(
                "CREATE MAPPING " + tableName + " ("
                        + " id INT, "
                        + " name VARCHAR "
                        + ") "
                        + "DATA CONNECTION " + TEST_DATABASE_REF
        );

        execute("UPDATE " + tableName + " SET name = ?", "updated");

        assertJdbcRowsAnyOrder(tableName,
                newArrayList(Integer.class, String.class),
                new Row(0, "updated")
        );
    }

    @Test
    public void updateTableSetUsingTableColumnWithExternalName() throws Exception {
        createTable(tableName);
        insertItems(tableName, 2);
        execute(
                "CREATE MAPPING " + tableName + " ("
                        + " person_id INT EXTERNAL NAME id, "
                        + " name VARCHAR "
                        + ") "
                        + "DATA CONNECTION " + TEST_DATABASE_REF
        );

        execute("UPDATE " + tableName + " SET name = 'updated-'||person_id");

        assertJdbcRowsAnyOrder(tableName,
                newArrayList(Integer.class, String.class),
                new Row(0, "updated-0"),
                new Row(1, "updated-1")
        );
    }

    @Test
    public void updateTableWhereOnNonPKColumnWithExternalName() throws Exception {
        createTable(tableName, "id INT PRIMARY KEY", "name VARCHAR(10)", "age INT");
        executeJdbc("INSERT INTO " + quote(tableName) + " VALUES(0, 'name-0', 20)");
        executeJdbc("INSERT INTO " + quote(tableName) + " VALUES(1, 'name-1', 20)");
        execute(
                "CREATE MAPPING " + tableName + " ("
                        + " id INT, "
                        + " fullName VARCHAR EXTERNAL NAME name,"
                        + " age INT "
                        + ") "
                        + "DATA CONNECTION " + TEST_DATABASE_REF
        );

        execute("UPDATE " + tableName + " SET age = 42 WHERE fullName='name-0'");

        assertJdbcRowsAnyOrder(tableName,
                newArrayList(Integer.class, String.class, Integer.class),
                new Row(0, "name-0", 42),
                new Row(1, "name-1", 20)
        );
    }

    @Test
    public void updateTableWhereAndSetUsingQueryParameter() throws Exception {
        createTable(tableName, "id INT PRIMARY KEY", "name VARCHAR(10)", "age INT");
        executeJdbc("INSERT INTO " + quote(tableName) + " VALUES(0, 'name-0', 20)");
        executeJdbc("INSERT INTO " + quote(tableName) + " VALUES(1, 'name-1', 20)");
        execute(
                "CREATE MAPPING " + tableName + " ("
                        + " id INT, "
                        + " name VARCHAR,"
                        + " age INT "
                        + ") "
                        + "DATA CONNECTION " + TEST_DATABASE_REF
        );

        execute("UPDATE " + tableName + " SET age = ? WHERE name = ?", 42, "name-0");

        assertJdbcRowsAnyOrder(tableName,
                newArrayList(Integer.class, String.class, Integer.class),
                new Row(0, "name-0", 42),
                new Row(1, "name-1", 20)
        );
    }

    @Test
    public void updateTableWhereOnPKAndSetUsingQueryParameter() throws Exception {
        createTable(tableName, "id INT PRIMARY KEY", "name VARCHAR(10)", "age INT");
        executeJdbc("INSERT INTO " + quote(tableName) + " VALUES(0, 'name-0', 20)");
        executeJdbc("INSERT INTO " + quote(tableName) + " VALUES(1, 'name-1', 20)");
        execute(
                "CREATE MAPPING " + tableName + " ("
                        + " id INT, "
                        + " name VARCHAR,"
                        + " age INT "
                        + ") "
                        + "DATA CONNECTION " + TEST_DATABASE_REF
        );

        execute("UPDATE " + tableName + " SET age = ?, name = ? WHERE id = ?", 42, "updated", 0);

        assertJdbcRowsAnyOrder(tableName,
                newArrayList(Integer.class, String.class, Integer.class),
                new Row(0, "updated", 42),
                new Row(1, "name-1", 20)
        );
    }

    @Test
    public void updateTableWithExternalName() throws Exception {
        createTable(tableName);
        insertItems(tableName, 2);

        String mappingName = "mapping_" + randomName();
        createMapping(tableName, mappingName);

        execute("UPDATE " + mappingName + " SET name = 'updated'");

        assertJdbcRowsAnyOrder(tableName,
                newArrayList(Integer.class, String.class),
                new Row(0, "updated"),
                new Row(1, "updated")
        );
    }

    @Test
    public void updateTableWithMultiplePKColumns() throws Exception {
        createTableNoQuote(quote(tableName),
                quote("id") + " INT", quote("id2") + " INT", quote("name") + " VARCHAR(10)",
                "PRIMARY KEY(" + quote("id") + ", " + quote("id2") + ")"
        );
        executeJdbc("INSERT INTO " + quote(tableName) + " VALUES(0, 0, 'name-0')");
        executeJdbc("INSERT INTO " + quote(tableName) + " VALUES(1, 0, 'name-1')");
        executeJdbc("INSERT INTO " + quote(tableName) + " VALUES(0, 1, 'name-2')");
        execute(
                "CREATE MAPPING " + tableName + " ("
                        + " id INT, "
                        + " id2 INT, "
                        + " name VARCHAR"
                        + ") "
                        + "DATA CONNECTION " + TEST_DATABASE_REF
        );

        execute("UPDATE " + tableName + " SET name = 'updated' WHERE id = 0 AND id2 = 1");

        assertJdbcRowsAnyOrder(tableName,
                newArrayList(Integer.class, Integer.class, String.class),
                new Row(0, 0, "name-0"),
                new Row(1, 0, "name-1"),
                new Row(0, 1, "updated")
        );
    }

    @Test
    public void updateTableWithMultiplePKColumnsNoPredicatePushDown() throws Exception {
        createTableNoQuote(quote(tableName),
                quote("id") + " INT", quote("id2") + " INT", quote("name") + " VARCHAR(10)",
                "PRIMARY KEY(" + quote("id") + ", " + quote("id2") + ")", quote("data") + " VARCHAR(100)"
        );

        executeJdbc("INSERT INTO " + quote(tableName) + " VALUES(0, 0, 'name-0', '{\"value\":0}')");
        executeJdbc("INSERT INTO " + quote(tableName) + " VALUES(1, 0, 'name-1', '{\"value\":1}')");
        executeJdbc("INSERT INTO " + quote(tableName) + " VALUES(0, 1, 'name-2', '{\"value\":2}')");
        execute(
                "CREATE MAPPING " + tableName + " ("
                        + " id INT, "
                        + " id2 INT, "
                        + " name VARCHAR, "
                        + " data VARCHAR"
                        + ") "
                        + "DATA CONNECTION " + TEST_DATABASE_REF
        );

        execute("UPDATE " + tableName + " SET name = 'updated' WHERE JSON_QUERY(data, '$.value') = '2'");

        assertJdbcQueryRowsAnyOrder("SELECT " + quote("name") + " FROM " + quote(tableName),
                new Row("name-0"),
                new Row("name-1"),
                new Row("updated")
        );
    }

    @Test
    public void updateTableWithReverseColumnOrder() throws Exception {
        createTable(tableName, "name VARCHAR(10)", "id INT PRIMARY KEY");
        executeJdbc("INSERT INTO " + quote(tableName) + " VALUES('name-0', 0)");
        executeJdbc("INSERT INTO " + quote(tableName) + " VALUES('name-1', 1)");
        execute(
                "CREATE MAPPING " + tableName + " ("
                        + " name VARCHAR, "
                        + " id INT "
                        + ") "
                        + "DATA CONNECTION " + TEST_DATABASE_REF
        );

        execute("UPDATE " + tableName + " SET name = 'updated' WHERE id = 0");

        assertJdbcRowsAnyOrder(tableName,
                newArrayList(String.class, Integer.class),
                new Row("updated", 0),
                new Row("name-1", 1)
        );
    }

    @Test
    public void updateMappingWithResolvedFields() throws Exception {
        createTable(tableName);
        insertItems(tableName, 1);

        execute("CREATE MAPPING " + tableName + " DATA CONNECTION " + TEST_DATABASE_REF);

        execute("UPDATE " + tableName + " SET name = 'updated' WHERE id = 0");
        assertJdbcRowsAnyOrder(tableName,
                newArrayList(Integer.class, String.class),
                new Row(0, "updated")
        );
    }

    @Test
    public void updateMappingWithQuotedColumnInWhere() throws Exception {
        createTable(tableName, "person-id INT PRIMARY KEY", "name VARCHAR(100)");
        insertItems(tableName, 1);

        execute("CREATE MAPPING " + tableName + " DATA CONNECTION " + TEST_DATABASE_REF);

        execute("UPDATE " + tableName + " SET name = 'updated' WHERE \"person-id\" = 0");
        assertJdbcRowsAnyOrder(tableName,
                newArrayList(Integer.class, String.class),
                new Row(0, "updated")
        );
    }

    @Test
    public void updateMappingWithQuotedColumnInSet() throws Exception {
        createTable(tableName, "id INT PRIMARY KEY", "full-name VARCHAR(100)");
        insertItems(tableName, 1);

        execute("CREATE MAPPING " + tableName + " DATA CONNECTION " + TEST_DATABASE_REF);

        execute("UPDATE " + tableName + " SET \"full-name\" = 'updated' WHERE id = 0");
        assertJdbcRowsAnyOrder(tableName,
                newArrayList(Integer.class, String.class),
                new Row(0, "updated")
        );
    }

}
