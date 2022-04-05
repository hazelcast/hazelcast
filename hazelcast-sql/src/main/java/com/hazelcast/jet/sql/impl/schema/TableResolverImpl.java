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
import com.hazelcast.jet.sql.impl.connector.infoschema.TablesTable;
import com.hazelcast.jet.sql.impl.connector.infoschema.ViewsTable;
import com.hazelcast.jet.sql.impl.connector.virtual.ViewTable;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.schema.ConstantTableStatistics;
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
public class TableResolverImpl implements TableResolver {

    public static final String SCHEMA_NAME_PUBLIC = "public";
    public static final String SCHEMA_NAME_INFORMATION_SCHEMA = "information_schema";

    private static final List<List<String>> SEARCH_PATHS = singletonList(
            asList(CATALOG, SCHEMA_NAME_PUBLIC)
    );

    private final NodeEngine nodeEngine;
    private final TablesStorage tableStorage;
    private final SqlConnectorCache connectorCache;
    private final List<TableListener> listeners;

    public TableResolverImpl(
            NodeEngine nodeEngine,
            TablesStorage tableStorage,
            SqlConnectorCache connectorCache
    ) {
        this.nodeEngine = nodeEngine;
        this.tableStorage = tableStorage;
        this.connectorCache = connectorCache;
        this.listeners = new CopyOnWriteArrayList<>();

        // because listeners are invoked asynchronously from the calling thread,
        // local changes are handled in createMapping() & removeMapping(), thus
        // we skip events originating from local member to avoid double processing
        this.tableStorage.registerListener(new TablesStorage.EntryListenerAdapter() {
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
            tableStorage.putIfAbsent(name, resolved);
        } else if (replace) {
            tableStorage.put(name, resolved);
            listeners.forEach(TableListener::onTableChanged);
        } else if (!tableStorage.putIfAbsent(name, resolved)) {
            throw QueryException.error("Mapping or view already exists: " + name);
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
        if (tableStorage.removeMapping(name) != null) {
            listeners.forEach(TableListener::onTableChanged);
        } else if (!ifExists) {
            throw QueryException.error("Mapping does not exist: " + name);
        }
    }

    @Nonnull
    public Collection<String> getMappingNames() {
        return tableStorage.mappingNames();
    }
    // endregion

    // region view

    public void createView(View view, boolean replace, boolean ifNotExists) {
        if (ifNotExists) {
            tableStorage.putIfAbsent(view.name(), view);
        } else if (replace) {
            tableStorage.put(view.name(), view);
        } else if (!tableStorage.putIfAbsent(view.name(), view)) {
            throw QueryException.error("Mapping or view already exists: " + view.name());
        }
    }

    public View getView(String name) {
        return tableStorage.getView(name);
    }

    public void removeView(String name, boolean ifExists) {
        if (tableStorage.removeView(name) == null && !ifExists) {
            throw QueryException.error("View does not exist: " + name);
        }
    }

    @Nonnull
    public Collection<String> getViewNames() {
        return tableStorage.viewNames();
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
        Collection<Object> objects = tableStorage.allObjects();
        List<Table> tables = new ArrayList<>(objects.size() + 3);
        for (Object o : objects) {
            if (o instanceof Mapping) {
                tables.add(toTable((Mapping) o));
            } else if (o instanceof View) {
                tables.add(toTable((View) o));
            } else {
                throw new RuntimeException("Unexpected: " + o);
            }
        }

        Collection<Mapping> mappings = objects.stream()
                .filter(o -> o instanceof Mapping)
                .map(m -> (Mapping) m)
                .collect(Collectors.toList());
        Collection<View> views = objects.stream()
                .filter(o -> o instanceof View)
                .map(v -> (View) v)
                .collect(Collectors.toList());
        tables.add(new TablesTable(CATALOG, SCHEMA_NAME_INFORMATION_SCHEMA, SCHEMA_NAME_PUBLIC, mappings, views));
        tables.add(new MappingsTable(CATALOG, SCHEMA_NAME_INFORMATION_SCHEMA, SCHEMA_NAME_PUBLIC, mappings));
        tables.add(new MappingColumnsTable(CATALOG, SCHEMA_NAME_INFORMATION_SCHEMA, SCHEMA_NAME_PUBLIC, mappings, views));
        tables.add(new ViewsTable(CATALOG, SCHEMA_NAME_INFORMATION_SCHEMA, SCHEMA_NAME_PUBLIC, views));
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

    private Table toTable(View view) {
        return new ViewTable(SCHEMA_NAME_PUBLIC, view.name(), view.query(), new ConstantTableStatistics(0L));
    }

    @Override
    public void registerListener(TableListener listener) {
        listeners.add(listener);
    }
}
