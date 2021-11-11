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

package com.hazelcast.jet.sql.impl.schema;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.jet.sql.impl.connector.SqlConnector;
import com.hazelcast.jet.sql.impl.connector.SqlConnectorCache;
import com.hazelcast.jet.sql.impl.connector.infoschema.MappingColumnsTable;
import com.hazelcast.jet.sql.impl.connector.infoschema.MappingsTable;
import com.hazelcast.jet.sql.impl.connector.infoschema.ViewsTable;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.schema.Mapping;
import com.hazelcast.sql.impl.schema.MappingField;
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.schema.TableResolver;
import com.hazelcast.sql.impl.schema.view.View;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import static com.hazelcast.sql.impl.QueryUtils.CATALOG;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

/**
 * A table resolver for DDL-created mappings and for the {@code
 * information_schema}.
 */
public class InformationSchemaCatalog implements TableResolver {

    public static final String SCHEMA_NAME_PUBLIC = "public";
    public static final String SCHEMA_NAME_INFORMATION_SCHEMA = "information_schema";

    private static final List<List<String>> SEARCH_PATHS = singletonList(
            asList(CATALOG, SCHEMA_NAME_PUBLIC)
    );

    private final NodeEngine nodeEngine;
    private final MappingStorage mappingStorage;
    private final ViewStorage viewStorage;
    private final SqlConnectorCache connectorCache;
    private final List<TableListener> listeners;

    public InformationSchemaCatalog(
            NodeEngine nodeEngine,
            MappingStorage mappingStorage,
            ViewStorage viewStorage,
            SqlConnectorCache connectorCache
    ) {
        this.nodeEngine = nodeEngine;
        this.mappingStorage = mappingStorage;
        this.viewStorage = viewStorage;
        this.connectorCache = connectorCache;
        this.listeners = new CopyOnWriteArrayList<>();

        // because listeners are invoked asynchronously from the calling thread,
        // local changes are handled in createMapping() & removeMapping(), thus
        // we skip events originating from local member to avoid double processing
        this.mappingStorage.registerListener(new MappingStorage.EntryListenerAdapter() {
            @Override
            public void entryUpdated(EntryEvent<String, Object> event) {
                if (!event.getMember().localMember()) {
                    listeners.forEach(TableListener::onTableChanged);
                }
            }

            @Override
            public void entryRemoved(EntryEvent<String, Object> event) {
                if (!event.getMember().localMember()) {
                    listeners.forEach(TableListener::onTableChanged);
                }
            }
        });
    }

    // region mapping

    public void createMapping(Mapping mapping, boolean replace, boolean ifNotExists) {
        Mapping resolved = resolveMapping(mapping);

        String name = resolved.name();
        if (ifNotExists) {
            mappingStorage.putIfAbsent(name, resolved);
        } else if (replace) {
            mappingStorage.put(name, resolved);
            listeners.forEach(TableListener::onTableChanged);
        } else if (!mappingStorage.putIfAbsent(name, resolved)) {
            throw QueryException.error("Mapping or view with such name exists: " + name);
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
                new LinkedHashMap<>(options)
        );
    }

    public void removeMapping(String name, boolean ifExists) {
        if (mappingStorage.remove(name) != null) {
            listeners.forEach(TableListener::onTableChanged);
        } else if (!ifExists) {
            throw QueryException.error("Mapping does not exist: " + name);
        }
    }

    @Nonnull
    public List<String> getMappingNames() {
        return mappingStorage.values().stream().map(Mapping::name).collect(Collectors.toList());
    }

    // endregion

    // region view

    public void createView(View view, boolean replace) {
        if (replace) {
            viewStorage.put(view.name(), view);
            return;
        }

        if (!viewStorage.putIfAbsent(view.name(), view)) {
            throw QueryException.error("Mapping or view with such name exists: " + view.name());
        }
    }

    public void removeView(String name, boolean ifExists) {
        if (viewStorage.remove(name) != null) {
            listeners.forEach(TableListener::onTableChanged);
        } else if (!ifExists) {
            throw QueryException.error("View does not exist: " + name);
        }
    }

    // endregion

    @Nonnull
    @Override
    public List<List<String>> getDefaultSearchPaths() {
        return SEARCH_PATHS;
    }

    @Nonnull
    @Override
    public List<Table> getTables() {
        Collection<Mapping> mappings = mappingStorage.values();
        List<Table> tables = new ArrayList<>(mappings.size() + 3);
        for (Mapping mapping : mappings) {
            tables.add(toTable(mapping));
        }
        tables.add(new MappingsTable(CATALOG, SCHEMA_NAME_INFORMATION_SCHEMA, SCHEMA_NAME_PUBLIC, mappings));
        tables.add(new MappingColumnsTable(CATALOG, SCHEMA_NAME_INFORMATION_SCHEMA, SCHEMA_NAME_PUBLIC, mappings));
        tables.add(new ViewsTable(CATALOG, SCHEMA_NAME_INFORMATION_SCHEMA, SCHEMA_NAME_PUBLIC, viewStorage.values()));
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

    @Override
    public void registerListener(TableListener listener) {
        listeners.add(listener);
    }
}
