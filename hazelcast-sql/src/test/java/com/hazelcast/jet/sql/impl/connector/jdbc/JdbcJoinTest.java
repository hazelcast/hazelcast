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
import com.hazelcast.test.jdbc.OracleDatabaseProvider;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.SQLException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
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

    @Test
    public void test_stream2BatchJoinAsNestedLoopJoinIsNotSupported() throws Exception {
        String tableName = randomTableName();
        createTable(tableName);
        insertItems(tableName, 5);

        execute(
                "CREATE MAPPING " + tableName + " ("
                        + " id INT, "
                        + " name VARCHAR "
                        + ") "
                        + "DATA CONNECTION " + TEST_DATABASE_REF
        );

        assertThatThrownBy(() ->
                sqlService.execute("SELECT n.name, t.v FROM " +
                        "TABLE(GENERATE_STREAM(2)) t " +
                        "JOIN " + tableName + " n ON n.id = t.v;")
        ).hasMessageContaining("JDBC connector doesn't support stream-to-batch JOIN");
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
        executeJdbc(databaseProvider.createSchemaQuery(schemaName));
        if (databaseProvider instanceof OracleDatabaseProvider) {
            executeJdbc("GRANT UNLIMITED TABLESPACE TO " + quote(schemaName));
        }
        String fullyQualifiedTable = quote(schemaName, tableName);
        createTableNoQuote(fullyQualifiedTable);
        insertItemsNoQuote(fullyQualifiedTable, ITEM_COUNT);
        String mappingName = randomTableName();
        createMapping(fullyQualifiedTable, mappingName);

        assertRowsAnyOrder(
                "SELECT t1.id, t2.name " +
                        "FROM " + tableName + " t1 " +
                        "JOIN " + mappingName + " t2 " +
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
