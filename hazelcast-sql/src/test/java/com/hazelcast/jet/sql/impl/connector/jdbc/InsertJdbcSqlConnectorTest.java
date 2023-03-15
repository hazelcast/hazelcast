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

import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.test.jdbc.H2DatabaseProvider;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.SQLException;

import static com.hazelcast.jet.sql.impl.connector.jdbc.JdbcSqlConnector.OPTION_DATA_LINK_NAME;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class InsertJdbcSqlConnectorTest extends JdbcSqlTestSupport {

    private String tableName;
    private String alternativeSchemaTable;

    @BeforeClass
    public static void beforeClass() {
        initialize(new H2DatabaseProvider());
    }

    @Before
    public void setUp() throws Exception {
        tableName = randomTableName();
        String schemaName = randomName();
        executeJdbc("CREATE SCHEMA " + schemaName);
        alternativeSchemaTable = schemaName + "." + tableName;
    }

    @Test
    public void insertIntoTable() throws Exception {
        createTable(tableName);
        createMapping(tableName);

        execute("INSERT INTO " + tableName + " VALUES (0, 'name-0')");

        assertJdbcRowsAnyOrder(tableName, new Row(0, "name-0"));
    }

    @Test
    public void insertIntoTableWithExternalName() throws Exception {
        createTable(tableName);
        String mappingName = "mapping_" + randomName();
        createMapping(tableName, mappingName);

        execute("INSERT INTO " + mappingName + " VALUES (0, 'name-0')");

        assertJdbcRowsAnyOrder(tableName, new Row(0, "name-0"));
    }

    @Test
    public void insertIntoTableColumnHasExternalName() throws Exception {
        createTable(tableName);
        execute(
                "CREATE MAPPING " + tableName + " ("
                        + " id INT, "
                        + " fullName VARCHAR EXTERNAL NAME name"
                        + ") "
                        + "TYPE " + JdbcSqlConnector.TYPE_NAME + ' '
                        + "OPTIONS ( "
                        + " '" + OPTION_DATA_LINK_NAME + "'='" + TEST_DATABASE_REF + "'"
                        + ")"
        );

        execute("INSERT INTO " + tableName + " VALUES (0, 'name-0')");

        assertJdbcRowsAnyOrder(tableName, new Row(0, "name-0"));
    }

    @Test
    public void insertIntoTableWithColumns() throws Exception {
        createTable(tableName);
        createMapping(tableName);

        execute("INSERT INTO " + tableName + " (name, id) VALUES ('name-0', 0), ('name-1', 1)");

        assertJdbcRowsAnyOrder(tableName,
                new Row(0, "name-0"),
                new Row(1, "name-1")
        );
    }

    @Test
    public void insertIntoTableWithColumnsColumnHasExternalName() throws Exception {
        createTable(tableName);
        execute(
                "CREATE MAPPING " + tableName + " ("
                        + " id INT, "
                        + " fullName VARCHAR EXTERNAL NAME name"
                        + ") "
                        + "TYPE " + JdbcSqlConnector.TYPE_NAME + ' '
                        + "OPTIONS ( "
                        + " '" + OPTION_DATA_LINK_NAME + "'='" + TEST_DATABASE_REF + "'"
                        + ")"
        );

        execute("INSERT INTO " + tableName + " (fullName, id) VALUES ('name-0', 0), ('name-1', 1)");

        assertJdbcRowsAnyOrder(tableName,
                new Row(0, "name-0"),
                new Row(1, "name-1")
        );
    }

    @Test
    public void insertIntoTableMultipleValues() throws Exception {
        createTable(tableName);
        createMapping(tableName);

        execute("INSERT INTO " + tableName + " SELECT v,'name-' || v FROM TABLE(generate_series(0,4))");

        assertJdbcRowsAnyOrder(tableName,
                new Row(0, "name-0"),
                new Row(1, "name-1"),
                new Row(2, "name-2"),
                new Row(3, "name-3"),
                new Row(4, "name-4")
        );
    }

    /**
     * H2 throws org.h2.jdbc.JdbcBatchUpdateException, after which the insert is retried, we can either handle
     * java.sql.BatchUpdateException as non-transient exception, or something else, not sure
     */
    @Test
    @Ignore
    public void insertIntoTableSameValues() throws Exception {
        createTable(tableName);
        createMapping(tableName);

        execute("INSERT INTO " + tableName + " VALUES (0, 'name-0')");

        assertThatThrownBy(() ->
                execute("INSERT INTO " + tableName + " VALUES (0, 'name-0')")
        ).isInstanceOf(HazelcastSqlException.class);
    }

    @Test
    public void insertIntoTableReverseColumnOrder() throws Exception {
        createTable(tableName, "id INT PRIMARY KEY", "name VARCHAR(10)");
        execute(
                "CREATE MAPPING " + tableName
                        + " TYPE " + JdbcSqlConnector.TYPE_NAME + ' '
                        + " OPTIONS ( "
                        + " '" + OPTION_DATA_LINK_NAME + "'='" + TEST_DATABASE_REF + "'"
                        + ")"
        );

        execute("INSERT INTO " + tableName + " (name, id) VALUES ('name-0', 0)");

        assertJdbcRowsAnyOrder(tableName,
                new Row(0, "name-0")
        );
    }

    @Test
    @Ignore("Requires https://github.com/hazelcast/hazelcast/pull/23634")
    public void insertIntoTableNonDefaultSchema() throws SQLException {
        createTable(alternativeSchemaTable);
        createMapping(alternativeSchemaTable);

        execute("INSERT INTO \"" + alternativeSchemaTable + "\" VALUES (0, 'name-0')");
        assertJdbcRowsAnyOrder(alternativeSchemaTable, new Row(0, "name-0"));
    }


    @Test
    @Ignore("Requires https://github.com/hazelcast/hazelcast/pull/23634")
    public void insertIntoTableWithExternalNameNonDefaultSchema() throws Exception {
        createTable(alternativeSchemaTable);
        String mappingName = "mapping_" + randomName();
        createMapping(alternativeSchemaTable, mappingName);

        execute("INSERT INTO " + mappingName + " VALUES (0, 'name-0')");

        assertJdbcRowsAnyOrder(alternativeSchemaTable, new Row(0, "name-0"));
    }
}
