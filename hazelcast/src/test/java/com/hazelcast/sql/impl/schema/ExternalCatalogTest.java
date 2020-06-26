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

package com.hazelcast.sql.impl.schema;

import com.google.common.collect.ImmutableMap;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.SqlTestSupport;
import com.hazelcast.sql.impl.connector.LocalPartitionedMapConnector;
import com.hazelcast.sql.impl.schema.ExternalTable.ExternalField;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Map;
import java.util.NoSuchElementException;

import static com.hazelcast.sql.impl.type.QueryDataType.INT;
import static com.hazelcast.sql.impl.type.QueryDataType.VARCHAR;
import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.junit.Assert.assertEquals;

public class ExternalCatalogTest extends SqlTestSupport {

    private static final TestHazelcastInstanceFactory FACTORY = new TestHazelcastInstanceFactory();

    private static HazelcastInstance instance;

    @BeforeClass
    public static void beforeClass() {
        instance = FACTORY.newHazelcastInstance();
    }

    @AfterClass
    public static void afterClass() {
        FACTORY.shutdownAll();
    }

    @Test(expected = QueryException.class)
    public void throws_whenTriesToCreateDuplicateTable() {
        // given
        String name = "my_table_to_create";
        ExternalCatalog catalog = new ExternalCatalog(nodeEngine(instance));
        catalog.createTable(table(name), false, false);

        // when
        // then
        catalog.createTable(table(name), false, false);
    }

    @Test
    public void replaces_whenTableAlreadyExists() {
        // given
        String name = "my_table_to_be_replaced";
        ExternalCatalog catalog = new ExternalCatalog(nodeEngine(instance));
        catalog.createTable(table(name, ImmutableMap.of("__key", INT, "this", VARCHAR)), true, false);

        // when
        ExternalTable table = table(name, ImmutableMap.of("__key", VARCHAR, "this", INT));
        catalog.createTable(table, true, false);

        // then
        assertEquals(ImmutableMap.of("__key", VARCHAR, "this", INT), tableFields(catalog, name));
    }

    @Test
    public void doesNotThrow_whenTableAlreadyExists() {
        // given
        String name = "my_table_if_not_exists";
        ExternalCatalog catalog = new ExternalCatalog(nodeEngine(instance));
        catalog.createTable(table(name, ImmutableMap.of("__key", INT, "this", VARCHAR)), false, true);

        // when
        ExternalTable table = table(name, ImmutableMap.of("__key", VARCHAR, "this", INT));
        catalog.createTable(table, false, true);

        // then
        assertEquals(ImmutableMap.of("__key", INT, "this", VARCHAR), tableFields(catalog, name));
    }

    @Test(expected = QueryException.class)
    public void throws_whenTableDoesNotExist() {
        // given
        ExternalCatalog catalog = new ExternalCatalog(nodeEngine(instance));

        // when
        // then
        catalog.removeTable("my_table_to_be_removed", false);
    }

    @Test
    public void doesNotThrow_whenTableDoesNotExist() {
        // given
        ExternalCatalog catalog = new ExternalCatalog(nodeEngine(instance));

        // when
        // then
        catalog.removeTable("my_table_if_exists", true);
    }

    private static ExternalTable table(String name) {
        return table(name, ImmutableMap.of("__key", INT, "this", VARCHAR));
    }

    private static ExternalTable table(String name, Map<String, QueryDataType> fields) {
        return new ExternalTable(
                name,
                LocalPartitionedMapConnector.TYPE_NAME,
                fields.entrySet().stream()
                      .map(entry -> new ExternalField(entry.getKey(), entry.getValue(), "this." + entry.getKey()))
                      .collect(toList()),
                emptyMap()
        );
    }

    private static Map<String, QueryDataType> tableFields(ExternalCatalog catalog, String tableName) {
        return catalog.getTables().stream()
                      .filter(table -> tableName.equals(table.getName()))
                      .findFirst()
                      .map(table -> table.getFields().stream().collect(toMap(TableField::getName, TableField::getType)))
                      .orElseThrow(NoSuchElementException::new);
    }
}
