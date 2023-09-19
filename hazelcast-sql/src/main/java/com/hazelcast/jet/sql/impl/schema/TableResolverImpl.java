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
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.jet.sql.impl.connector.SqlConnector;
import com.hazelcast.jet.sql.impl.connector.SqlConnectorCache;
import com.hazelcast.jet.sql.impl.connector.infoschema.MappingColumnsTable;
import com.hazelcast.jet.sql.impl.connector.infoschema.MappingsTable;
import com.hazelcast.jet.sql.impl.connector.infoschema.TablesTable;
import com.hazelcast.jet.sql.impl.connector.infoschema.ViewsTable;
import com.hazelcast.jet.sql.impl.connector.virtual.ViewTable;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.schema.BadTable;
import com.hazelcast.sql.impl.schema.ConstantTableStatistics;
import com.hazelcast.sql.impl.schema.Mapping;
import com.hazelcast.sql.impl.schema.MappingField;
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.schema.TableResolver;
import com.hazelcast.sql.impl.schema.type.Type;
import com.hazelcast.sql.impl.schema.view.View;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiFunction;

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

    private static final List<BiFunction<List<Mapping>, List<View>, Table>> ADDITIONAL_TABLE_PRODUCERS = Arrays.asList(
            (m, v) -> new TablesTable(CATALOG, SCHEMA_NAME_INFORMATION_SCHEMA, SCHEMA_NAME_PUBLIC, m, v),
            (m, v) -> new MappingsTable(CATALOG, SCHEMA_NAME_INFORMATION_SCHEMA, SCHEMA_NAME_PUBLIC, m),
            (m, v) -> new MappingColumnsTable(CATALOG, SCHEMA_NAME_INFORMATION_SCHEMA, SCHEMA_NAME_PUBLIC, m, v),
            (m, v) -> new ViewsTable(CATALOG, SCHEMA_NAME_INFORMATION_SCHEMA, SCHEMA_NAME_PUBLIC, v)
    );

    private final NodeEngine nodeEngine;
    private final TablesStorage tableStorage;
    private final SqlConnectorCache connectorCache;
    private final List<TableListener> listeners;

    // These fields should normally be volatile because we're accessing them from multiple threads. But we
    // don't care if some thread doesn't see a newer value written by another thread. Each thread will write
    // the same value (we assume that the number of mappings and views doesn't change much), so we
    // shave a tiny bit of performance from not synchronizing :)
    private int lastViewsSize;
    private int lastMappingsSize;

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
        nodeEngine.getHazelcastInstance().getLifecycleService().addLifecycleListener(event -> {
            if (event.getState() == LifecycleEvent.LifecycleState.STARTED) {
                this.tableStorage.initializeWithListener(new TablesStorage.EntryListenerAdapter() {
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
        List<MappingField> resolvedFields = connector.resolveAndValidateFields(
                nodeEngine,
                options,
                mapping.fields(),
                mapping.externalName()
        );
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

    // region type

    public Collection<String> getTypeNames() {
        return tableStorage.typeNames();
    }

    public Collection<Type> getTypes() {
        return tableStorage.getAllTypes();
    }

    public void createType(Type type, boolean replace, boolean ifNotExists) {
        if (ifNotExists) {
            tableStorage.putIfAbsent(type.getName(), type);
        } else if (replace) {
            tableStorage.put(type.getName(), type);
        } else if (!tableStorage.putIfAbsent(type.getName(), type)) {
            throw QueryException.error("Type already exists: " + type.getName());
        }
    }

    public void removeType(String name, boolean ifExists) {
        if (tableStorage.removeType(name) == null && !ifExists) {
            throw QueryException.error("Type does not exist: " + name);
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
        Collection<Object> objects = tableStorage.allObjects();
        List<Table> tables = new ArrayList<>(objects.size() + ADDITIONAL_TABLE_PRODUCERS.size());

        int lastMappingsSize = this.lastMappingsSize;
        int lastViewsSize = this.lastViewsSize;

        // Trying to avoid list growing.
        List<Mapping> mappings = lastMappingsSize == 0 ? new ArrayList<>() : new ArrayList<>(lastMappingsSize);
        List<View> views = lastViewsSize == 0 ? new ArrayList<>() : new ArrayList<>(lastViewsSize);

        for (Object o : objects) {
            if (o instanceof Mapping) {
                tables.add(toTable((Mapping) o));
                mappings.add((Mapping) o);
            } else if (o instanceof View) {
                tables.add(toTable((View) o));
                views.add((View) o);
            } else if (o instanceof Type) {
                // Types are not tables
                continue;
            } else {
                throw new RuntimeException("Unexpected: " + o);
            }
        }

        ADDITIONAL_TABLE_PRODUCERS.forEach(
                producer -> tables.add(producer.apply(mappings, views)));

        this.lastViewsSize = views.size();
        this.lastMappingsSize = mappings.size();

        return tables;
    }

    private Table toTable(Mapping mapping) {
        SqlConnector connector = connectorCache.forType(mapping.type());
        try {
            return connector.createTable(
                    nodeEngine,
                    SCHEMA_NAME_PUBLIC,
                    mapping.name(),
                    mapping.externalName(),
                    mapping.options(),
                    mapping.fields()
            );
        } catch (Throwable e) {
            // will fail later if invalid table is actually used in a query
            return new BadTable(SCHEMA_NAME_PUBLIC, mapping.name(),
                    e instanceof QueryException ? e.getCause() : e);
        }
    }

    private Table toTable(View view) {
        return new ViewTable(SCHEMA_NAME_PUBLIC, view.name(), view.query(), new ConstantTableStatistics(0L));
    }

    @Override
    public void registerListener(TableListener listener) {
        listeners.add(listener);
    }
}
