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
import com.hazelcast.sql.impl.connector.SqlConnector;
import com.hazelcast.sql.impl.connector.SqlConnectorCache;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.hazelcast.sql.impl.QueryUtils.CATALOG;
import static com.hazelcast.sql.impl.QueryUtils.SCHEMA_NAME_PUBLIC;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class ExternalCatalog implements TableResolver {

    // TODO: is it the best/right name?
    public static final String CATALOG_MAP_NAME = "__sql.catalog";

    private static final List<List<String>> SEARCH_PATHS = singletonList(asList(CATALOG, SCHEMA_NAME_PUBLIC));

    private final NodeEngine nodeEngine;
    private final SqlConnectorCache sqlConnectorCache;

    public ExternalCatalog(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.sqlConnectorCache = new SqlConnectorCache(nodeEngine);
    }

    public void createTable(ExternalTable table, boolean replace, boolean ifNotExists) {
        // catch all the potential errors - missing connector, class, invalid format or field definitions etc. - early
        try {
            toTable(table);
        } catch (Exception e) {
            throw QueryException.error("Invalid table definition: " + e.getMessage(), e);
        }

        String name = table.name();
        if (ifNotExists) {
            tables().putIfAbsent(name, table);
        } else if (replace) {
            tables().put(name, table);
        } else if (tables().putIfAbsent(name, table) != null) {
            throw QueryException.error("'" + name + "' table already exists");
        }
    }

    public void removeTable(String name, boolean ifExists) {
        if (tables().remove(name) == null && !ifExists) {
            throw QueryException.error("'" + name + "' table does not exist");
        }
    }

    @Override
    public List<List<String>> getDefaultSearchPaths() {
        return SEARCH_PATHS;
    }

    @Override @Nonnull
    public Collection<Table> getTables() {
        return tables().values().stream()
                       .map(this::toTable)
                       .collect(toList());
    }

    private Map<String, ExternalTable> tables() {
        // TODO: use the right storage
        return nodeEngine.getHazelcastInstance().getReplicatedMap(CATALOG_MAP_NAME);
    }

    private Table toTable(ExternalTable table) {
        String type = table.type();
        SqlConnector connector = requireNonNull(sqlConnectorCache.forType(type), "Unknown connector type - '" + type + "'");
        return connector.createTable(nodeEngine, SCHEMA_NAME_PUBLIC, table.name(), table.options(), table.fields());
    }
}
