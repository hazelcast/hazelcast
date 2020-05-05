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

import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.extract.GenericQueryTargetDescriptor;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.schema.map.MapTableField;
import com.hazelcast.sql.impl.schema.map.PartitionedMapTable;
import com.hazelcast.sql.impl.schema.map.ReplicatedMapTable;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static com.hazelcast.sql.impl.schema.SchemaUtils.CATALOG;
import static com.hazelcast.sql.impl.schema.SchemaUtils.SCHEMA_NAME_PUBLIC;
import static com.hazelcast.sql.impl.schema.map.PartitionedMapTable.DISTRIBUTION_FIELD_ORDINAL_NONE;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;

public class Catalog implements TableResolver {

    public static final String CATALOG_MAP_NAME = "hz:impl:sql:catalog"; // TODO:

    // TODO: dynamic/class based resolution ???
    private static final String PARTITIONED_TYPE = "PARTITIONED";
    private static final String REPLICATED_TYPE = "REPLICATED";

    private static final List<List<String>> SEARCH_PATHS =
            Collections.singletonList(Arrays.asList(CATALOG, SCHEMA_NAME_PUBLIC));

    private final NodeEngine nodeEngine;

    public Catalog(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
    }

    public void createTable(TableSchema schema, boolean replace, boolean ifNotExists) {
        assert schema.type().equalsIgnoreCase(PARTITIONED_TYPE) || schema.type().equalsIgnoreCase(REPLICATED_TYPE); // TODO: dynamic/class based resolution ???

        Map<String, TableSchema> tables = nodeEngine.getHazelcastInstance().getReplicatedMap(CATALOG_MAP_NAME);

        String name = schema.name();
        if (ifNotExists) {
            tables.putIfAbsent(name, schema);
        } else if (replace) {
            tables.put(name, schema);
        } else if (tables.putIfAbsent(name, schema) != null) {
            throw QueryException.error("'" + name + "' table already exists");
        }
    }

    public void removeTable(String name, boolean ifExists) {
        Map<String, TableSchema> tables = nodeEngine.getHazelcastInstance().getReplicatedMap(CATALOG_MAP_NAME);

        if (tables.remove(name) == null && !ifExists) {
            throw new IllegalArgumentException("'" + name + "' table does not exist");
        }
    }

    @Override
    public List<List<String>> getDefaultSearchPaths() {
        return SEARCH_PATHS;
    }

    @Override
    public Collection<Table> getTables() {
        Map<String, TableSchema> tables = nodeEngine.getHazelcastInstance().getReplicatedMap(CATALOG_MAP_NAME);

        return tables.values().stream()
                     .map(Catalog::toTable)
                     .collect(toList());
    }

    private static Table toTable(TableSchema schema) {
        // TODO: dynamic/class based resolution ???
        if (PARTITIONED_TYPE.equalsIgnoreCase(schema.type())) {
            return new PartitionedMapTable(SCHEMA_NAME_PUBLIC, schema.name(), toMapFields(schema.fields()), new ConstantTableStatistics(0),
                    new GenericQueryTargetDescriptor(), new GenericQueryTargetDescriptor(), emptyList(), DISTRIBUTION_FIELD_ORDINAL_NONE); // TODO: ???
        } else if (REPLICATED_TYPE.equalsIgnoreCase(schema.type())) {
            return new ReplicatedMapTable(SCHEMA_NAME_PUBLIC, schema.name(), toMapFields(schema.fields()), new ConstantTableStatistics(0),
                    new GenericQueryTargetDescriptor(), new GenericQueryTargetDescriptor()); // TODO: ???
        } else {
            throw new IllegalArgumentException("Unknown table type - " + schema.type());
        }
    }

    private static List<TableField> toMapFields(List<Entry<String, QueryDataType>> fields) {
        return fields.stream()
                     .map(field -> new MapTableField(field.getKey(), field.getValue(), QueryPath.create(field.getKey()))) // TODO: ???
                     .collect(toList());
    }
}
