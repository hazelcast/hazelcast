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
import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * Implementation of a schema, exposing sub schemas and tables.
 */
public class HazelcastSchema implements Schema {

    private final Map<String, Schema> subSchemaMap;
    private final Map<String, Table> tableMap;

    public HazelcastSchema(Map<String, Table> tableMap) {
        this(null, tableMap);
    }

    public HazelcastSchema(Map<String, Schema> subSchemaMap, Map<String, Table> tableMap) {
        this.subSchemaMap = subSchemaMap != null ? subSchemaMap : Collections.emptyMap();
        this.tableMap = tableMap != null ? tableMap : Collections.emptyMap();
    }

    protected Map<String, Schema> getSubSchemaMap() {
        return subSchemaMap;
    }

    final public Map<String, Table> getTableMap() {
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
        //noinspection RedundantCast
        return (Set<String>) getTableMap().keySet();
    }

    @Override public final @Nullable Table getTable(String name) {
        return getTableMap().get(name);
    }

    /**
     * Returns a map of types in this schema by name.
     *
     * <p>The implementations of {@link #getTypeNames()}
     * and {@link #getType(String)} depend on this map.
     * The default implementation of this method returns the empty map.
     * Override this method to change their behavior.
     *
     * @return Map of types in this schema by name
     */
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

    /**
     * Returns a multi-map of functions in this schema by name.
     * It is a multi-map because functions are overloaded; there may be more than
     * one function in a schema with a given name (as long as they have different
     * parameter lists).
     *
     * <p>The implementations of {@link #getFunctionNames()}
     * and {@link Schema#getFunctions(String)} depend on this map.
     * The default implementation of this method returns the empty multi-map.
     * Override this method to change their behavior.
     *
     * @return Multi-map of functions in this schema by name
     */
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

    /** Schema factory that creates an
     * {@link org.apache.calcite.schema.impl.AbstractSchema}. */
    public static class Factory implements SchemaFactory {
        public static final Factory INSTANCE = new Factory();

        private Factory() {}

        @Override public Schema create(SchemaPlus parentSchema, String name,
                                       Map<String, Object> operand) {
            return new AbstractSchema();
        }
    }
}
