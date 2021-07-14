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

    private static SqlService sqlService;

    private final String mappingName = randomName();
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
                        + "__value VARCHAR EXTERNAL NAME \"this.value\""
                        + ") TYPE " + IMapSqlConnector.TYPE_NAME + "\n"
                        + "OPTIONS (\n"
                        + '\'' + OPTION_KEY_FORMAT + "'='int'\n"
                        + ", '" + OPTION_VALUE_FORMAT + "'='" + JAVA_FORMAT + "'\n"
                        + ", '" + OPTION_VALUE_CLASS + "'='" + Value.class.getName() + "'\n"
                        + ")"
        );
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
    public void test_columns() {
        assertRowsAnyOrder(
                "SELECT * FROM information_schema.columns",
                asList(
                        new Row("hazelcast", "public", mappingName, "__key", "__key", 1, "true", "INTEGER"),
                        new Row("hazelcast", "public", mappingName, "__value", "this.value", 2, "true", "VARCHAR")
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
                singletonList(new Row(mappingName, "HAZELCAST-P", "__value", "VARCHAR"))
        );
    }

    @Test
    public void when_predicateAndProjectionIsUsed_then_correctRowsAndColumnsAreReturned() {
        assertRowsAnyOrder(
                "SELECT table_name, UPPER(table_catalog), column_name, data_type "
                        + "FROM information_schema.columns "
                        + "WHERE column_name = '__value'",
                singletonList(
                        new Row(mappingName, "HAZELCAST", "__value", "VARCHAR")
                )
        );
    }

    @Test
    public void test_planCache_mappings() {
        assertRowsAnyOrder(
                "SELECT mapping_name FROM information_schema.mappings",
                singletonList(new Row(mappingName))
        );
        assertThat(planCache(instance()).size()).isEqualTo(1);

        assertRowsAnyOrder(
                "SELECT mapping_external_name FROM information_schema.mappings",
                singletonList(new Row(mappingExternalName))
        );
        assertThat(planCache(instance()).size()).isEqualTo(2);
    }

    @Test
    public void test_planCache_columns() {
        assertRowsAnyOrder(
                "SELECT column_name FROM information_schema.columns WHERE ordinal_position = 2",
                singletonList(new Row("__value"))
        );
        assertThat(planCache(instance()).size()).isEqualTo(1);

        assertRowsAnyOrder(
                "SELECT column_external_name FROM information_schema.columns WHERE ordinal_position = 2",
                singletonList(new Row("this.value"))
        );
        assertThat(planCache(instance()).size()).isEqualTo(2);
    }

    public static final class Value {
        public String value;
    }
}
