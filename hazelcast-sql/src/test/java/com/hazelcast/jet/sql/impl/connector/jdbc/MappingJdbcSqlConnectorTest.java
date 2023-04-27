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

import com.google.common.collect.ImmutableList;
import com.hazelcast.config.Config;
import com.hazelcast.config.DataConnectionConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.dataconnection.impl.InternalDataConnectionService;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlColumnMetadata;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlRowMetadata;
import com.hazelcast.test.jdbc.H2DatabaseProvider;
import com.hazelcast.test.jdbc.MySQLDatabaseProvider;
import com.hazelcast.test.jdbc.PostgresDatabaseProvider;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.assertj.core.util.Lists.newArrayList;

public class MappingJdbcSqlConnectorTest extends JdbcSqlTestSupport {

    private static final String LE = System.lineSeparator();

    protected String tableName;

    @BeforeClass
    public static void beforeClass() {
        initialize(new H2DatabaseProvider());
    }

    @Before
    public void setUp() throws Exception {
        tableName = randomTableName();
    }

    @Test
    public void createMappingWithExternalTableName() throws Exception {
        createTable(tableName);

        String mappingName = "mapping_" + randomName();
        createMapping(tableName, mappingName);

        assertRowsAnyOrder("SHOW MAPPINGS",
                newArrayList(new Row(mappingName))
        );
    }

    @Test
    public void createMappingWithExternalSchemaAndTableName() throws Exception {
        executeJdbc("CREATE SCHEMA schema1");
        createTable("schema1." + tableName);

        String mappingName = "mapping_" + randomName();
        createMapping("\"schema1\".\"" + tableName + '\"', mappingName);

        assertRowsAnyOrder("SHOW MAPPINGS",
                newArrayList(new Row(mappingName))
        );

        String expectedMappingQuery = "CREATE OR REPLACE EXTERNAL MAPPING \"hazelcast\".\"public\"" +
                ".\"" + mappingName + "\" EXTERNAL NAME \"schema1\"" +
                ".\"" + tableName + "\" (" + LE +
                "  \"id\" INTEGER," + LE +
                "  \"name\" VARCHAR" + LE +
                ")" + LE +
                "DATA CONNECTION \"testDatabaseRef\"";
        assertRowsAnyOrder("SELECT GET_DDL('relation', '" + mappingName + "')",
                ImmutableList.of(new Row(expectedMappingQuery)));
    }

    @Test
    public void createMappingWithExternalTableNameTooManyComponents() throws Exception {
        createTable(tableName);

        assertThatThrownBy(() ->
                execute("CREATE MAPPING " + tableName
                        + " EXTERNAL NAME \"aaaa\".\"bbbb\".\"cccc\".\"dddd\" "
                        + " ("
                        + " id INT, "
                        + " name VARCHAR "
                        + ") "
                        + "DATA CONNECTION " + TEST_DATABASE_REF
                )
        ).isInstanceOf(HazelcastSqlException.class)
         .hasMessageContaining("Invalid external name \"aaaa\".\"bbbb\".\"cccc\".\"dddd\"");
    }

    @Test
    public void createMappingWithExternalTableNameTooManyComponentsNoQuotes() throws Exception {
        createTable(tableName);

        assertThatThrownBy(() ->
                execute("CREATE MAPPING " + tableName
                        + " EXTERNAL NAME aaaa.bbbb.cccc.dddd "
                        + " ("
                        + " id INT, "
                        + " name VARCHAR "
                        + ") "
                        + "DATA CONNECTION " + TEST_DATABASE_REF
                )
        ).isInstanceOf(HazelcastSqlException.class)
         .hasMessageContaining("Invalid external name \"aaaa\".\"bbbb\".\"cccc\".\"dddd\"");
    }

    @Test
    public void createMappingWithoutDataConnection() {
        tableName = randomTableName();

        assertThatThrownBy(() ->
                execute("CREATE MAPPING " + tableName
                        + " EXTERNAL NAME " + tableName
                        + " ("
                        + " id INT, "
                        + " name VARCHAR "
                        + ") "
                        + "TYPE " + JdbcSqlConnector.TYPE_NAME
                )
        ).isInstanceOf(HazelcastSqlException.class)
         .hasMessageContaining("You must provide data connection when using the Jdbc connector");
    }

    @Test
    public void createMappingTableDoesNotExist() {
        tableName = randomTableName();
        assertThatThrownBy(() -> execute(
                "CREATE MAPPING " + tableName + " ("
                        + " id INT, "
                        + " name VARCHAR "
                        + ") "
                        + "DATA CONNECTION " + TEST_DATABASE_REF
        ))
                .isInstanceOf(HazelcastSqlException.class);


        assertRowsAnyOrder("SHOW MAPPINGS",
                emptyList()
        );
    }

    @Test
    public void createMappingNoColumnsTableDoesNotExist() {
        tableName = randomTableName();
        assertThatThrownBy(() -> execute(
                "CREATE MAPPING " + tableName + " DATA CONNECTION " + TEST_DATABASE_REF
        )).isInstanceOf(HazelcastSqlException.class);

        assertRowsAnyOrder("SHOW MAPPINGS",
                emptyList()
        );
    }

    @Test
    public void createMappingWithExternalFieldName() throws Exception {
        createTable(tableName);

        execute("CREATE MAPPING " + tableName
                + " ("
                + " id INT, "
                + " fullName VARCHAR EXTERNAL NAME name "
                + ") "
                + "DATA CONNECTION " + TEST_DATABASE_REF
        );

        assertRowsAnyOrder("SHOW MAPPINGS",
                newArrayList(new Row(tableName))
        );

        insertItems(tableName, 1);
        assertRowsAnyOrder(
                "SELECT id,fullName FROM " + tableName,
                newArrayList(new Row(0, "name-0"))
        );
    }

    @Test
    public void createMappingFieldDoesNotExist() throws Exception {
        createTable(tableName);

        assertThatThrownBy(() ->
                execute("CREATE MAPPING " + tableName
                        + " ("
                        + " id INT, "
                        + " fullName VARCHAR "
                        + ") "
                        + "DATA CONNECTION " + TEST_DATABASE_REF
                )
        ).isInstanceOf(HazelcastSqlException.class)
         .hasMessageContaining("Could not resolve field with name fullName");

        assertRowsAnyOrder("SHOW MAPPINGS",
                emptyList()
        );
    }

    @Test
    public void createMappingExternalFieldDoesNotExist() throws Exception {
        tableName = randomTableName();
        try (Connection conn = DriverManager.getConnection(dbConnectionUrl);
             Statement stmt = conn.createStatement()
        ) {
            stmt.execute("CREATE TABLE " + tableName + " (id INT PRIMARY KEY, name VARCHAR(10))");
        }

        assertThatThrownBy(() ->
                sqlService.execute("CREATE MAPPING " + tableName
                        + " ("
                        + " id INT, "
                        + " fullName VARCHAR EXTERNAL NAME myName "
                        + ") "
                        + "DATA CONNECTION " + TEST_DATABASE_REF
                )
        ).isInstanceOf(HazelcastSqlException.class)
         .hasMessageContaining("Could not resolve field with external name myName");

        assertRowsAnyOrder("SHOW MAPPINGS",
                emptyList()
        );
    }

    @Test
    public void createMappingFieldTypesDoNotMatch() throws Exception {
        createTable(tableName);

        assertThatThrownBy(() ->
                execute("CREATE MAPPING " + tableName
                        + " ("
                        + " id BOOLEAN, "
                        + " name VARCHAR "
                        + ") "
                        + "DATA CONNECTION " + TEST_DATABASE_REF
                )
        ).isInstanceOf(HazelcastSqlException.class)
         .hasMessageContaining("Type BOOLEAN of field id does not match db type INTEGER");

        assertRowsAnyOrder("SHOW MAPPINGS",
                emptyList()
        );
    }

    @Test
    public void when_createMappingWithImplicitFieldTypesDefinition_then_orderIsPreserved() throws Exception {
        createTable(tableName);
        insertItems(tableName, 2);

        execute("CREATE MAPPING " + tableName
                + " DATA CONNECTION " + TEST_DATABASE_REF
        );

        // If you change LinkedHashMap -> HashMap at JdbcSqlConnector:159, it will fail.
        assertRowsAnyOrder(
                "SELECT * FROM " + tableName,
                asList(new Row(0, "name-0"), new Row(1, "name-1"))
        );
    }

    @Test
    public void when_mappingIsDeclaredWithDataConnection_then_itIsAvailable() throws Exception {
        // given
        String dlName = randomName();
        String name = randomName();
        createTable(name);
        Map<String, String> options = new HashMap<>();
        options.put("jdbcUrl", dbConnectionUrl);

        createDataConnection(instance(), dlName, "JDBC", false, options);
        InternalDataConnectionService dlService = getNodeEngineImpl(instance()).getDataConnectionService();
        assertThat(dlService.existsSqlDataConnection(dlName)).isTrue();

        createJdbcMappingUsingDataConnection(name, dlName);
        SqlResult mappings = sqlService.execute("SHOW MAPPINGS");
        Iterator<SqlRow> resultIt = mappings.iterator();
        assertThat(resultIt.hasNext()).isTrue();

        Object object = resultIt.next().getObject(0);
        assertThat(resultIt.hasNext()).isFalse();
        assertThat(object).isEqualTo(name);
    }

    @Test
    public void createMappingAutoResolveColumns() throws Exception {
        createTable(tableName);

        execute("CREATE MAPPING " + tableName
                + " DATA CONNECTION " + TEST_DATABASE_REF
        );

        try (SqlResult result = sqlService.execute("SELECT * FROM " + tableName)) {
            SqlRowMetadata metadata = result.getRowMetadata();

            assertThat(metadata.getColumns())
                    .extracting(SqlColumnMetadata::getName)
                    .contains("id", "name");
        }
    }

    // TEXT is not a standard SQL column type. This test may not run on all DBs
    @Test
    public void createMappingWithTextColumnType() throws Exception {
        assumeThat(databaseProvider)
                .isInstanceOfAny(
                        MySQLDatabaseProvider.class,
                        PostgresDatabaseProvider.class,
                        H2DatabaseProvider.class
                );

        executeJdbc("CREATE TABLE " + tableName + " (id INTEGER NOT NULL,name TEXT NOT NULL)");

        insertItems(tableName, 1);

        assertThatCode(() ->
                execute("CREATE MAPPING myMapping "
                        + " EXTERNAL NAME " + tableName
                        + " ("
                        + "id INTEGER,"
                        + " name VARCHAR "
                        + ") "
                        + "DATA CONNECTION " + TEST_DATABASE_REF
                )
        ).doesNotThrowAnyException();

        assertRowsAnyOrder(
                "SELECT * FROM myMapping",
                Collections.singletonList(new Row(0, "name-0"))
        );
    }

    // Postgres + MySQL : Test that table in another DB exist
    @Test
    public void createMappingFails_tableExistInAnotherDatabase_externalNameOnlyTableName() throws SQLException {
        assumeThat(databaseProvider)
                .isInstanceOfAny(
                        MySQLDatabaseProvider.class,
                        PostgresDatabaseProvider.class
                );
        HazelcastInstance instance = instance();
        Config config = instance.getConfig();

        // Create table on first DB
        createTable(tableName);

        String newDBName = "db1";
        executeJdbc("CREATE DATABASE " + newDBName);

        // Add a new DB
        String newDbUrl = dbConnectionUrl.replace("com.hazelcast.jet.sql.impl.connector.jdbc.JdbcSqlTestSupport",
                newDBName);

        Properties properties = new Properties();
        properties.setProperty("jdbcUrl", newDbUrl);

        String NEW_TEST_DATABASE_REF = "testDatabaseRef1";

        config.addDataConnectionConfig(
                new DataConnectionConfig(NEW_TEST_DATABASE_REF)
                        .setType("jdbc")
                        .setProperties(properties)
        );

        // Create mapping to new DB. Table does not exist on new DB, and we should get an exception
        assertThatThrownBy(() -> execute(
                "CREATE MAPPING " + tableName + " EXTERNAL NAME " + tableName + " ("
                + " id INT, "
                + " name VARCHAR "
                + ") "
                + "DATA CONNECTION " + NEW_TEST_DATABASE_REF
        ))
                .isInstanceOf(HazelcastSqlException.class)
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessageContaining("Could not execute readDbFields for table");
    }

    // Postgres : Test that table in another DB and explicit schema name exists
    @Test
    public void createMappingFails_tableExistInAnotherDatabase_externalNameFullName() throws SQLException {
        assumeThat(databaseProvider)
                .isInstanceOfAny(
                        PostgresDatabaseProvider.class
                );
        HazelcastInstance instance = instance();
        Config config = instance.getConfig();

        // Create table on first DB
        createTable(tableName);

        String newDBName = "db2";
        executeJdbc("CREATE DATABASE " + newDBName);

        // Add a new DB
        String newDbUrl = dbConnectionUrl.replace("com.hazelcast.jet.sql.impl.connector.jdbc.JdbcSqlTestSupport",
                newDBName);

        Properties properties = new Properties();
        properties.setProperty("jdbcUrl", newDbUrl);

        String NEW_TEST_DATABASE_REF = "testDatabaseRef2";

        config.addDataConnectionConfig(
                new DataConnectionConfig(NEW_TEST_DATABASE_REF)
                        .setType("jdbc")
                        .setProperties(properties)
        );

        // Create mapping to new DB. Table does not exist on new DB, and we should get an exception
        assertThatThrownBy(() -> execute(
                "CREATE MAPPING " + tableName + " EXTERNAL NAME " + newDBName + ".public." + tableName + " ("
                + " id INT, "
                + " name VARCHAR "
                + ") "
                + "DATA CONNECTION " + NEW_TEST_DATABASE_REF
        ))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessageContaining("Could not execute readDbFields for table");
    }
}
