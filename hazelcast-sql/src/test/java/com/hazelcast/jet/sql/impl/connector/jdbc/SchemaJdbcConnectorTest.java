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


import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.jdbc.H2DatabaseProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.util.Lists.newArrayList;

@RunWith(HazelcastParametrizedRunner.class)
@Category(QuickTest.class)
public class SchemaJdbcConnectorTest extends JdbcSqlTestSupport {

    @Parameter
    public String schema;

    @Parameter(value = 1)
    public String table;

    @Parameter(value = 2)
    public String externalName;

    protected String tableFull;

    @Parameters(name = "{index}: schemaName={0}, tableName={1}, externalTableName={2}")
    public static List<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {
                        "schema1",
                        "table1",
                        "schema1.table1",
                },
                {
                        "schema-with-hyphen",
                        "table-with-hyphen",
                        "\"schema-with-hyphen\".\"table-with-hyphen\"",
                },
                {
                        "schema with space",
                        "table with space",
                        "\"schema with space\".\"table with space\"",
                },
                {
                        "schema.with.dot",
                        "table1",
                        "\"schema.with.dot\".\"table1\"",
                },
                {
                        "schema1",
                        "table.with.dot",
                        "\"schema1\".\"table.with.dot\"",
                },
                {
                        "schema_with_quote\"",
                        "table_with_quote\"",
                        "\"schema_with_quote\"\"\".\"table_with_quote\"\"\"",
                },
                {
                        "schema_with_backtick`",
                        "table_with_backtick`",
                        "\"schema_with_backtick`\".\"table_with_backtick`\"",
                }
        });
    }

    @BeforeClass
    public static void beforeClass() {
        initialize(new H2DatabaseProvider());
    }

    @Before
    public void setUp() throws Exception {
        tableFull = quote(schema, table);
        try {
            executeJdbc(databaseProvider.createSchemaQuery(schema));
        } catch (Exception e) {
            logger.info("Could not create schema", e);
        }
        createTableNoQuote(tableFull);
    }

    @After
    public void after() throws SQLException {
        try {
            executeJdbc("DROP TABLE " + tableFull);
        } catch (Exception e) {
            logger.info("Could not drop schema", e);
        }
    }

    protected void myCreateMapping(String mappingName) {
        execute(
                "CREATE MAPPING \"" + mappingName + "\""
                        + " EXTERNAL NAME " + externalName + ""
                        + " ("
                        + " id INT, "
                        + " name VARCHAR "
                        + ") "
                        + "DATA CONNECTION " + TEST_DATABASE_REF
        );
    }

    @Test
    public void selectFromTableWithSchema() throws Exception {
        insertItemsNoQuote(tableFull, 1);

        String mappingName = "mapping_" + randomName();
        myCreateMapping(mappingName);

        assertRowsAnyOrder(
                "SELECT * FROM " + mappingName,
                newArrayList(
                        new Row(0, "name-0")
                )
        );
    }

    @Test
    public void insertIntoTableWithSchema() {
        String mappingName = "mapping_" + randomName();
        myCreateMapping(mappingName);

        execute("INSERT INTO " + mappingName + " VALUES (0, 'name-0')");

        assertJdbcRowsAnyOrderNoQuote(tableFull,
                newArrayList(Integer.class, String.class),
                new Row(0, "name-0"));
    }

    @Test
    public void updateTableWithSchema() throws Exception {
        insertItemsNoQuote(tableFull, 1);
        String mappingName = "mapping_" + randomName();
        myCreateMapping(mappingName);

        execute("UPDATE " + mappingName + " SET name = 'updated'");

        assertJdbcRowsAnyOrderNoQuote(tableFull,
                newArrayList(Integer.class, String.class),
                new Row(0, "updated"));
    }

    @Test
    public void deleteFromTableWithSchema() throws Exception {
        insertItemsNoQuote(tableFull, 1);
        String mappingName = "mapping_" + randomName();
        myCreateMapping(mappingName);

        execute("DELETE FROM " + mappingName);
        assertJdbcRowsAnyOrderNoQuote(tableFull, newArrayList(Integer.class, String.class));
    }

    @Test
    public void sinkIntoTableWithSchema() throws Exception {
        String mappingName = "mapping_" + randomName();
        myCreateMapping(mappingName);

        execute("SINK INTO " + mappingName + " VALUES (0, 'name-0')");

        assertJdbcRowsAnyOrderNoQuote(tableFull,
                newArrayList(Integer.class, String.class),
                new Row(0, "name-0"));
    }
}
