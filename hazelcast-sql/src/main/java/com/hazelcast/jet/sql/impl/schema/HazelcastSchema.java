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

package com.hazelcast.jet.sql.impl.schema;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.hazelcast.sql.impl.schema.SqlCatalog;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.jet.sql.impl.schema.HazelcastSchemaUtils.createTableStatistic;
import static java.util.Objects.requireNonNull;

/**
 * Implementation of a schema, exposing sub schemas and tables.
 */
public class HazelcastSchema implements Schema {

    private final Map<String, Schema> subSchemaMap;
    private Map<String, Table> tableMap;
    private SqlCatalog catalog;
    //TODO: Maybe having a reference to pattern is a better idea?
    private String schemaName;

    public HazelcastSchema(Map<String, Table> tableMap) {
        this(null, tableMap);
    }

    public HazelcastSchema(SqlCatalog catalog) {
        this.catalog = catalog;
        this.subSchemaMap = new HashMap<>();
        this.tableMap = new HashMap<>();
    }

    public HazelcastSchema(Map<String, Schema> subSchemaMap, Map<String, Table> tableMap) {
        this.subSchemaMap = subSchemaMap != null ? subSchemaMap : Collections.emptyMap();
        this.tableMap = tableMap != null ? tableMap : Collections.emptyMap();
    }

    public HazelcastSchema(Map<String, Schema> subSchemaMap, Map<String, Table> tableMap, SqlCatalog catalog, String schemaName) {
        this.subSchemaMap = subSchemaMap != null ? subSchemaMap : Collections.emptyMap();
        this.tableMap = tableMap != null ? tableMap : Collections.emptyMap();
        this.catalog = catalog;
        this.schemaName = schemaName;
    }

    //CHECK: Remove that as now it might be dangerous?
    protected Map<String, Schema> getSubSchemaMap() {
        return subSchemaMap;
    }

    //CHECK: Remove that as now it might be dangerous?
    public final Map<String, Table> getTableMap() {
        return tableMap;
    }

    @Override
    public Schema snapshot(SchemaVersion version) {
        return this;
    }

    @Override public boolean isMutable() {
        return true;
    }

    @Override public Expression getExpression(@Nullable SchemaPlus parentSchema, String name) {
        requireNonNull(parentSchema, "parentSchema");
        return Schemas.subSchemaExpression(parentSchema, name, getClass());
    }

    @Override public final Set<String> getTableNames() {
        return (Set<String>) getTableMap().keySet();
    }

    @Override public final @Nullable Table getTable(String name) {
        if (tableMap.containsKey(name) && tableMap.get(name) != null) {
            return tableMap.get(name);
        }

        com.hazelcast.sql.impl.schema.Table table = catalog.getSchemas().get(schemaName).get(name);
        HazelcastTable convertedTable = new HazelcastTable(
                table,
                createTableStatistic(table)
        );

        tableMap.put(name, convertedTable);
        return convertedTable;
    }

    protected Map<String, RelProtoDataType> getTypeMap() {
        return ImmutableMap.of();
    }

    @Override public @Nullable RelProtoDataType getType(String name) {
        return getTypeMap().get(name);
    }

    @Override public Set<String> getTypeNames() {
        //noinspection RedundantCast
        return (Set<String>) getTypeMap().keySet();
    }

    protected Multimap<String, Function> getFunctionMultimap() {
        return ImmutableMultimap.of();
    }

    @Override public final Collection<Function> getFunctions(String name) {
        return getFunctionMultimap().get(name); // never null
    }

    @Override public final Set<String> getFunctionNames() {
        return getFunctionMultimap().keySet();
    }

    @Override public final Set<String> getSubSchemaNames() {
        //noinspection RedundantCast
        return (Set<String>) getSubSchemaMap().keySet();
    }

    @Override public final @Nullable Schema getSubSchema(String name) {
        return getSubSchemaMap().get(name);
    }

    public static class Factory implements SchemaFactory {
        public static final Factory INSTANCE = new Factory();

        private Factory() { }

        @Override public Schema create(SchemaPlus parentSchema, String name,
                                       Map<String, Object> operand) {
            return new AbstractSchema();
        }
    }
}
