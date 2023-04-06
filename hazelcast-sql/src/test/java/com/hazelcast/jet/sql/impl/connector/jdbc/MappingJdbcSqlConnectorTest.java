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
import com.hazelcast.datalink.impl.InternalDataLinkService;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlColumnMetadata;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlRowMetadata;
import com.hazelcast.test.jdbc.H2DatabaseProvider;
import org.assertj.core.api.Condition;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static com.hazelcast.jet.sql.impl.connector.jdbc.JdbcSqlConnector.OPTION_DATA_LINK_NAME;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.util.Lists.emptyList;
import static org.assertj.core.util.Lists.newArrayList;

public class MappingJdbcSqlConnectorTest extends JdbcSqlTestSupport {

    private static final String LE = System.lineSeparator();

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
                "TYPE \"JDBC\"" + LE +
                "OPTIONS (" + LE +
                "  'data-link-name'='test-database-ref'" + LE +
                ")";
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
                        + "TYPE " + JdbcSqlConnector.TYPE_NAME + ' '
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
                        + "TYPE " + JdbcSqlConnector.TYPE_NAME + ' '
                )
        ).isInstanceOf(HazelcastSqlException.class)
         .hasMessageContaining("Invalid external name \"aaaa\".\"bbbb\".\"cccc\".\"dddd\"");
    }

    @Test
    public void createMappingWithoutDataLinkRef() {
        tableName = randomTableName();

        assertThatThrownBy(() ->
                execute("CREATE MAPPING " + tableName
                        + " EXTERNAL NAME " + tableName
                        + " ("
                        + " id INT, "
                        + " name VARCHAR "
                        + ") "
                        + "TYPE " + JdbcSqlConnector.TYPE_NAME + ' '
                )
        ).isInstanceOf(HazelcastSqlException.class)
         .hasMessageContaining("Missing option: 'data-link-name' must be set");
    }

    @Test
    public void createMappingTableDoesNotExist() {
        tableName = randomTableName();
        assertThatThrownBy(() -> execute(
                "CREATE MAPPING " + tableName + " ("
                        + " id INT, "
                        + " name VARCHAR "
                        + ") "
                        + "TYPE " + JdbcSqlConnector.TYPE_NAME + ' '
                        + "OPTIONS ( "
                        + " '" + OPTION_DATA_LINK_NAME + "'='" + TEST_DATABASE_REF + "'"
                        + ")"
        ))
                .isInstanceOf(HazelcastSqlException.class);


        assertRowsAnyOrder("SHOW MAPPINGS",
                emptyList()
        );
    }

    private Condition<Throwable> hasMessage(String message) {
        return new Condition<Throwable>() {
            @Override
            public boolean matches(Throwable value) {
                return value.getMessage().contains(message) ||
                        (value.getCause() != null && matches(value.getCause()));
            }
        };
    }

    @Test
    public void createMappingWithExternalFieldName() throws Exception {
        createTable(tableName);

        execute("CREATE MAPPING " + tableName
                + " ("
                + " id INT, "
                + " fullName VARCHAR EXTERNAL NAME name "
                + ") "
                + "TYPE " + JdbcSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ( "
                + " '" + OPTION_DATA_LINK_NAME + "'='" + TEST_DATABASE_REF + "'"
                + ")"
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
                        + "TYPE " + JdbcSqlConnector.TYPE_NAME + ' '
                        + "OPTIONS ( "
                        + " '" + OPTION_DATA_LINK_NAME + "'='" + TEST_DATABASE_REF + "'"
                        + ")"
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
                        + "TYPE " + JdbcSqlConnector.TYPE_NAME + ' '
                        + "OPTIONS ( "
                        + " '" + OPTION_DATA_LINK_NAME + "'='" + TEST_DATABASE_REF + "'"
                        + ")"
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
                        + "TYPE " + JdbcSqlConnector.TYPE_NAME + ' '
                        + "OPTIONS ( "
                        + " '" + OPTION_DATA_LINK_NAME + "'='" + TEST_DATABASE_REF + "'"
                        + ")"
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
                + " TYPE " + JdbcSqlConnector.TYPE_NAME + ' '
                + " OPTIONS ( "
                + " '" + OPTION_DATA_LINK_NAME + "'='" + TEST_DATABASE_REF + "'"
                + ")"
        );

        // If you change LinkedHashMap -> HashMap at JdbcSqlConnector:159, it will fail.
        assertRowsAnyOrder(
                "SELECT * FROM " + tableName,
                asList(new Row(0, "name-0"), new Row(1, "name-1"))
        );
    }

    @Test
    public void when_mappingIsDeclaredWithDataLink_then_itIsAvailable() {
        // given
        String dlName = randomName();
        String name = randomName();
        Map<String, String> options = new HashMap<>();
        options.put("jdbcUrl", dbConnectionUrl);

        createDataLink(instance(), dlName, "JDBC", false, options);
        InternalDataLinkService dlService = getNodeEngineImpl(instance()).getDataLinkService();
        assertThat(dlService.existsSqlDataLink(dlName)).isTrue();

        createJdbcMappingUsingDataLink(name, dlName);
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
                + " TYPE " + JdbcSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ( "
                + " '" + OPTION_DATA_LINK_NAME + "'='" + TEST_DATABASE_REF + "'"
                + ")"
        );

        try (SqlResult result = sqlService.execute("SELECT * FROM " + tableName)) {
            SqlRowMetadata metadata = result.getRowMetadata();

            assertThat(metadata.getColumns())
                    .extracting(SqlColumnMetadata::getName)
                    .contains("id", "name");
        }
    }
}
