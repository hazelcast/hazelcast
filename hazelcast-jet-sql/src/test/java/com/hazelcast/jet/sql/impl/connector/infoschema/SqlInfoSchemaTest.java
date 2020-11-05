/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
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

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

/**
 * Tests for the {@code information_schema}.
 */
public class SqlInfoSchemaTest extends SqlTestSupport {

    private static SqlService sqlService;

    private final String name = randomName();

    @BeforeClass
    public static void setUpClass() {
        initialize(1, null);
        sqlService = instance().getSql();
    }

    @Before
    public void setUp() {
        sqlService.execute(javaSerializableMapDdl(name, Integer.class, String.class));
    }

    @Test
    public void test_mappings() {
        // when
        assertRowsAnyOrder(
                "SELECT * FROM information_schema.mappings",
                singletonList(
                        new Row(
                                "hazelcast",
                                "public",
                                name,
                                IMapSqlConnector.TYPE_NAME,
                                "{"
                                        + "keyFormat=java"
                                        + ", keyJavaClass=java.lang.Integer"
                                        + ", valueFormat=java"
                                        + ", valueJavaClass=java.lang.String"
                                        + "}")
                )
        );
    }

    @Test
    public void test_columns() {
        assertRowsAnyOrder(
                "SELECT * FROM information_schema.columns",
                asList(
                        new Row("hazelcast", "public", name, "__key", 0, "true", "INTEGER"),
                        new Row("hazelcast", "public", name, "this", 1, "true", "VARCHAR")
                )
        );
    }

    @Test
    public void when_predicateAndProjectionIsUsed_then_correctRowsAndColumnsAreReturned() {
        assertRowsAnyOrder(
                "SELECT table_name, UPPER(table_catalog), column_name, data_type "
                        + "FROM information_schema.columns "
                        + "WHERE column_name = 'this'",
                singletonList(
                        new Row(name, "HAZELCAST", "this", "VARCHAR")
                )
        );
    }
}
