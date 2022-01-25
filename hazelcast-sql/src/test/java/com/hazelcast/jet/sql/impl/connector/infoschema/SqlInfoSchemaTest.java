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

package com.hazelcast.jet.sql.impl.connector.infoschema;

import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.jet.sql.impl.connector.map.IMapSqlConnector;
import com.hazelcast.sql.SqlService;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.hazelcast.jet.sql.impl.connector.SqlConnector.JAVA_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_CLASS;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FORMAT;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the {@code information_schema}.
 */
public class SqlInfoSchemaTest extends SqlTestSupport {

    private static final String LE = System.lineSeparator();

    private static SqlService sqlService;

    private final String mappingName = randomName();
    private final String viewName = randomName();
    private final String mappingExternalName = "my_map";

    @BeforeClass
    public static void setUpClass() {
        initialize(1, null);
        sqlService = instance().getSql();
    }

    @Before
    public void setUp() {
        sqlService.execute(
                "CREATE MAPPING " + mappingName + " EXTERNAL NAME " + mappingExternalName + "("
                        + "__key INT"
                        + ", __value VARCHAR EXTERNAL NAME \"this.value\""
                        + ") TYPE " + IMapSqlConnector.TYPE_NAME + "\n"
                        + "OPTIONS (\n"
                        + '\'' + OPTION_KEY_FORMAT + "'='int'\n"
                        + ", '" + OPTION_VALUE_FORMAT + "'='" + JAVA_FORMAT + "'\n"
                        + ", '" + OPTION_VALUE_CLASS + "'='" + Value.class.getName() + "'\n"
                        + ")");
        sqlService.execute("CREATE VIEW " + viewName + " AS SELECT * FROM " + mappingName);
    }

    @Test
    public void test_tables() {
        assertRowsAnyOrder(
                "SELECT * FROM information_schema.tables",
                asList(
                        new Row("hazelcast", "public", mappingName,
                                "BASE TABLE",
                                null, null, null, null, null,
                                "YES", "NO", null),
                        new Row("hazelcast", "public", viewName,
                                "VIEW",
                                null, null, null, null, null,
                                "NO", "NO", null)));
    }

    @Test
    public void test_mappings() {
        assertRowsAnyOrder(
                "SELECT * FROM information_schema.mappings",
                singletonList(
                        new Row(
                                "hazelcast",
                                "public",
                                mappingName,
                                mappingExternalName,
                                IMapSqlConnector.TYPE_NAME,
                                "{"
                                        + "\"keyFormat\":\"int\""
                                        + ",\"valueFormat\":\"java\""
                                        + ",\"valueJavaClass\":\"" + Value.class.getName() + "\""
                                        + "}")
                )
        );
    }

    @Test
    public void test_views() {
        assertRowsAnyOrder(
                "SELECT * FROM information_schema.views",
                singletonList(new Row(
                        "hazelcast",
                        "public",
                        viewName,
                        "SELECT \"" + mappingName + "\".\"__key\", \"" + mappingName + "\".\"__value\"" + LE +
                                "FROM \"hazelcast\".\"public\".\"" + mappingName + "\" AS \"" + mappingName + '"',
                        "NONE",
                        "NO",
                        "NO")));
    }

    @Test
    public void test_columns() {
        assertRowsAnyOrder(
                "SELECT * FROM information_schema.columns",
                asList(
                        new Row("hazelcast", "public", mappingName, "__key", "__key", 1, "true", "INTEGER"),
                        new Row("hazelcast", "public", mappingName, "__value", "this.value", 2, "true", "VARCHAR"),
                        new Row("hazelcast", "public", viewName, "__key", null, 1, "true", "INTEGER"),
                        new Row("hazelcast", "public", viewName, "__value", null, 2, "true", "VARCHAR")
                )
        );
    }

    @Test
    public void test_dynamicParameters() {
        assertRowsAnyOrder(
                "SELECT table_name, UPPER(table_catalog || ?), column_name, data_type "
                        + "FROM information_schema.columns "
                        + "WHERE column_name = ?",
                asList("-p", "__value"),
                asList(
                        new Row(mappingName, "HAZELCAST-P", "__value", "VARCHAR"),
                        new Row(viewName, "HAZELCAST-P", "__value", "VARCHAR"))
        );
    }

    @Test
    public void when_predicateAndProjectionIsUsed_then_correctRowsAndColumnsAreReturned() {
        assertRowsAnyOrder(
                "SELECT table_name, UPPER(table_catalog), column_name, data_type "
                        + "FROM information_schema.columns "
                        + "WHERE column_name = '__value'",
                asList(
                        new Row(mappingName, "HAZELCAST", "__value", "VARCHAR"),
                        new Row(viewName, "HAZELCAST", "__value", "VARCHAR")
                )
        );
    }

    @Test
    public void test_join() {
        assertRowsAnyOrder(
                "SELECT table_name, column_name " +
                        "FROM information_schema.mappings " +
                        "JOIN information_schema.columns USING (table_catalog, table_schema, table_name)",
                rows(2,
                        mappingName, "__key",
                        mappingName, "__value")
        );
    }

    @Test
    public void test_joinAndUnion() {
        assertRowsAnyOrder(
                "SELECT table_name, column_name " +
                        "FROM (" +
                        "    SELECT table_catalog, table_schema, table_name FROM information_schema.mappings " +
                        "    UNION ALL" +
                        "    SELECT table_catalog, table_schema, table_name FROM information_schema.views " +
                        ") " +
                        "JOIN information_schema.columns USING (table_catalog, table_schema, table_name)",
                rows(2,
                        mappingName, "__key",
                        mappingName, "__value",
                        viewName, "__key",
                        viewName, "__value")
        );
    }

    @Test
    public void test_planCache_mappings() {
        assertRowsAnyOrder(
                "SELECT table_name FROM information_schema.mappings",
                singletonList(new Row(mappingName))
        );
        assertThat(planCache(instance()).size()).isZero();
    }

    @Test
    public void test_planCache_columns() {
        sqlService.execute("SELECT column_name FROM information_schema.columns WHERE ordinal_position = 2");
        assertThat(planCache(instance()).size()).isZero();
    }

    public static final class Value {
        public String value;
    }
}
