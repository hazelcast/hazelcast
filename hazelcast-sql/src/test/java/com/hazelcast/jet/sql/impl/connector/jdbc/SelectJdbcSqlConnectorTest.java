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

import static org.assertj.core.util.Lists.emptyList;
import static org.assertj.core.util.Lists.newArrayList;

public class SelectJdbcSqlConnectorTest extends JdbcSqlTestSupport {

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
    public void selectAllFromTable() {
        assertRowsAnyOrder(
                "SELECT * FROM " + tableName,
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
    public void selectColumnFromTable() {
        assertRowsAnyOrder(
                "SELECT id AS c FROM " + tableName,
                newArrayList(
                        new Row(0),
                        new Row(1),
                        new Row(2),
                        new Row(3),
                        new Row(4)
                )
        );
    }

    @Test
    public void selectConcatExpressionFromTable() {
        assertRowsAnyOrder(
                "SELECT id||'-'||name AS c FROM " + tableName,
                newArrayList(
                        new Row("0-name-0"),
                        new Row("1-name-1"),
                        new Row("2-name-2"),
                        new Row("3-name-3"),
                        new Row("4-name-4")
                )
        );
    }

    @Test
    public void selectColumnAsFromTable() {
        assertRowsAnyOrder(
                "SELECT id AS person_id FROM " + tableName,
                newArrayList(
                        new Row(0),
                        new Row(1),
                        new Row(2),
                        new Row(3),
                        new Row(4)
                )
        );
    }

    @Test
    public void selectColumnWithExternalNameFromTable() throws Exception {
        tableName = randomTableName();
        createTable(tableName);
        insertItems(tableName, ITEM_COUNT);
        execute(
                "CREATE MAPPING " + tableName + " ("
                        + " id INT, "
                        + " fullName VARCHAR EXTERNAL NAME name"
                        + ") "
                        + "DATA CONNECTION " + TEST_DATABASE_REF
        );

        assertRowsAnyOrder(
                "SELECT id,fullName FROM " + tableName,
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
    public void selectAllFromTableWhereIdColumn() {
        assertRowsAnyOrder(
                "SELECT * FROM " + tableName + " WHERE id = ?",
                newArrayList(0),
                newArrayList(
                        new Row(0, "name-0")
                )
        );
    }

    @Test
    public void selectFromTableWhereColumnWithExternalName() throws Exception {
        tableName = randomTableName();
        createTable(tableName);
        insertItems(tableName, ITEM_COUNT);
        execute(
                "CREATE MAPPING " + tableName + " ("
                        + " id INT, "
                        + " fullName VARCHAR EXTERNAL NAME name"
                        + ") "
                        + "DATA CONNECTION " + TEST_DATABASE_REF
        );

        assertRowsAnyOrder(
                "SELECT * FROM " + tableName + " WHERE fullName = 'name-0'",
                newArrayList(
                        new Row(0, "name-0")
                )
        );
    }

    @Test
    public void selectColumnDifferentTypeInMappingAndTable() throws Exception {
        tableName = randomTableName();
        createTable(tableName);
        insertItems(tableName, ITEM_COUNT);
        execute(
                "CREATE MAPPING " + tableName + " ("
                        + " id VARCHAR, " // The type in database table is INT, but it's convertible
                        + " name VARCHAR "
                        + ") "
                        + "DATA CONNECTION " + TEST_DATABASE_REF
        );

        assertRowsAnyOrder(
                "SELECT * FROM " + tableName,
                newArrayList(
                        new Row("0", "name-0"),
                        new Row("1", "name-1"),
                        new Row("2", "name-2"),
                        new Row("3", "name-3"),
                        new Row("4", "name-4")
                )
        );
    }

    @Test
    public void selectAllFromTableWhereIdIn() {
        assertRowsAnyOrder(
                "SELECT * FROM " + tableName + " WHERE id IN (?, ?, ?) ",
                newArrayList(0, 1, 2),
                newArrayList(
                        new Row(0, "name-0"),
                        new Row(1, "name-1"),
                        new Row(2, "name-2")
                )
        );
    }

    @Test
    public void selectFromEmptyTable() throws Exception {
        String emptyTableName = randomTableName();
        createTable(emptyTableName);
        execute(
                "CREATE MAPPING " + emptyTableName + " ("
                        + " id INT, "
                        + " name VARCHAR "
                        + ") "
                        + "DATA CONNECTION " + TEST_DATABASE_REF
        );

        assertRowsAnyOrder(
                "SELECT * FROM " + emptyTableName,
                emptyList()
        );
    }

    @Test
    public void selectFromTableWhereClauseDoesNotMatchAnyResult() {
        assertRowsAnyOrder(
                "SELECT * FROM " + tableName + " WHERE id = ?",
                newArrayList(42), // only ids 0-4 exist
                emptyList() // so we expect empty result
        );
    }

}
