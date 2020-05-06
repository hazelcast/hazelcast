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
import com.hazelcast.sql.impl.connector.Connector;
import com.hazelcast.sql.impl.connector.ConnectorFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.hazelcast.sql.impl.schema.SchemaUtils.CATALOG;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;

public class Catalog implements TableResolver {

    public static final String CATALOG_MAP_NAME = "__sql.catalog"; // TODO:

    private static final List<List<String>> SEARCH_PATHS = singletonList(singletonList(CATALOG));

    private final NodeEngine nodeEngine;

    public Catalog(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
    }

    public void createTable(TableSchema schema, boolean replace, boolean ifNotExists) {
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
        Connector connector = ConnectorFactory.from(schema.type());
        return connector.createTable("", schema.name(), schema.fields(), schema.options());
    }
}
