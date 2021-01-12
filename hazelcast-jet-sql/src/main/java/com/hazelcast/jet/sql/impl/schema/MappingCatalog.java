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

package com.hazelcast.jet.sql.impl.schema;

import com.hazelcast.jet.sql.impl.connector.SqlConnector;
import com.hazelcast.jet.sql.impl.connector.SqlConnectorCache;
import com.hazelcast.jet.sql.impl.connector.infoschema.MappingColumnsTable;
import com.hazelcast.jet.sql.impl.connector.infoschema.MappingsTable;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.schema.TableResolver;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.hazelcast.sql.impl.QueryUtils.CATALOG;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

/**
 * A table resolver for DDL-created mappings and for the {@code
 * information_schema}.
 */
public class MappingCatalog implements TableResolver {

    public static final String SCHEMA_NAME_PUBLIC = "public";
    public static final String SCHEMA_NAME_INFORMATION_SCHEMA = "information_schema";

    private static final List<List<String>> SEARCH_PATHS = singletonList(
            asList(CATALOG, SCHEMA_NAME_PUBLIC)
    );

    private final NodeEngine nodeEngine;
    private final MappingStorage storage;
    private final SqlConnectorCache connectorCache;

    public MappingCatalog(
            NodeEngine nodeEngine,
            MappingStorage storage,
            SqlConnectorCache connectorCache
    ) {
        this.nodeEngine = nodeEngine;
        this.storage = storage;
        this.connectorCache = connectorCache;
    }

    public void createMapping(Mapping mapping, boolean replace, boolean ifNotExists) {
        Mapping resolved = resolveMapping(mapping);

        String name = resolved.name();
        if (ifNotExists) {
            storage.putIfAbsent(name, resolved);
        } else if (replace) {
            storage.put(name, resolved);
        } else if (!storage.putIfAbsent(name, resolved)) {
            throw QueryException.error("Mapping already exists: " + name);
        }
    }

    private Mapping resolveMapping(Mapping mapping) {
        String type = mapping.type();
        Map<String, String> options = mapping.options();

        SqlConnector connector = connectorCache.forType(type);
        List<MappingField> resolvedFields = connector.resolveAndValidateFields(nodeEngine, options, mapping.fields());
        return new Mapping(
                mapping.name(),
                mapping.externalName(),
                type,
                new ArrayList<>(resolvedFields),
                new HashMap<>(options)
        );
    }

    public void removeMapping(String name, boolean ifExists) {
        if (!storage.remove(name) && !ifExists) {
            throw QueryException.error("Mapping does not exist: " + name);
        }
    }

    @Nonnull
    public List<String> getMappingNames() {
        return storage.values().stream().map(Mapping::name).collect(Collectors.toList());
    }

    @Nonnull
    @Override
    public List<List<String>> getDefaultSearchPaths() {
        return SEARCH_PATHS;
    }

    @Nonnull
    @Override
    public List<Table> getTables() {
        List<Table> tables = new ArrayList<>();
        List<MappingDefinition> infoSchemaData = new ArrayList<>();
        for (Mapping mapping : storage.values()) {
            Table table = toTable(mapping);
            MappingDefinition definition = new MappingDefinition(table, mapping.type(), mapping.options());

            tables.add(table);
            infoSchemaData.add(definition);
        }
        tables.add(new MappingsTable(CATALOG, SCHEMA_NAME_INFORMATION_SCHEMA, infoSchemaData));
        tables.add(new MappingColumnsTable(CATALOG, SCHEMA_NAME_INFORMATION_SCHEMA, infoSchemaData));
        return tables;
    }

    private Table toTable(Mapping mapping) {
        SqlConnector connector = connectorCache.forType(mapping.type());
        return connector.createTable(
                nodeEngine,
                SCHEMA_NAME_PUBLIC,
                mapping.name(),
                mapping.externalName(),
                mapping.options(),
                mapping.fields()
        );
    }
}
