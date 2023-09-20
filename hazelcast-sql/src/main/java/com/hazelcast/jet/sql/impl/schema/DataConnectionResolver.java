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

import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.dataconnection.DataConnection;
import com.hazelcast.dataconnection.impl.DataConnectionServiceImpl;
import com.hazelcast.dataconnection.impl.DataConnectionServiceImpl.DataConnectionSource;
import com.hazelcast.dataconnection.impl.InternalDataConnectionService;
import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.util.Preconditions;
import com.hazelcast.jet.function.TriFunction;
import com.hazelcast.jet.sql.impl.connector.SqlConnectorCache;
import com.hazelcast.jet.sql.impl.connector.infoschema.DataConnectionsTable;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.schema.TableResolver;
import com.hazelcast.sql.impl.schema.dataconnection.DataConnectionCatalogEntry;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static com.hazelcast.sql.impl.QueryUtils.CATALOG;
import static com.hazelcast.sql.impl.QueryUtils.SCHEMA_NAME_INFORMATION_SCHEMA;
import static com.hazelcast.sql.impl.QueryUtils.SCHEMA_NAME_PUBLIC;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;

public class DataConnectionResolver implements TableResolver {
    // It will be in a separate schema, so separate resolver is implemented.
    private static final List<List<String>> SEARCH_PATHS = singletonList(
            asList(CATALOG, SCHEMA_NAME_PUBLIC)
    );

    @SuppressWarnings("checkstyle:LineLength")
    private static final List<TriFunction<List<DataConnectionCatalogEntry>, SqlConnectorCache, Boolean, Table>> ADDITIONAL_TABLE_PRODUCERS = singletonList(
            (dl, connectorCache, securityEnabled) -> new DataConnectionsTable(
                    CATALOG,
                    SCHEMA_NAME_INFORMATION_SCHEMA,
                    SCHEMA_NAME_PUBLIC,
                    dl,
                    connectorCache,
                    securityEnabled
            ));

    private final DataConnectionStorage dataConnectionStorage;
    private final DataConnectionServiceImpl dataConnectionService;
    private final SqlConnectorCache connectorCache;
    private final boolean isSecurityEnabled;
    private final CopyOnWriteArrayList<TableListener> listeners;

    public DataConnectionResolver(
            InternalDataConnectionService dataConnectionService,
            SqlConnectorCache connectorCache,
            DataConnectionStorage dataConnectionStorage,
            boolean isSecurityEnabled
    ) {
        Preconditions.checkInstanceOf(DataConnectionServiceImpl.class, dataConnectionService);
        this.dataConnectionService = (DataConnectionServiceImpl) dataConnectionService;
        this.dataConnectionStorage = dataConnectionStorage;
        this.connectorCache = connectorCache;
        this.isSecurityEnabled = isSecurityEnabled;

        // See comment in TableResolverImpl regarding local events processing.
        // We rely on the fact that both are data connections and tables
        // are in the same IMap and can share (in fact they must) listener logic.
        // Currently, onTableChanged event is used also for data connections
        // (they affect mappings) but separate event may be created if needed.
        this.listeners = new CopyOnWriteArrayList<>();
    }

    /**
     * @return true, if the data connection was created
     */
    public boolean createDataConnection(DataConnectionCatalogEntry dl, boolean replace, boolean ifNotExists) {
        if (replace) {
            dataConnectionStorage.put(dl.name(), dl);
            listeners.forEach(TableListener::onTableChanged);
            return true;
        } else {
            boolean added = dataConnectionStorage.putIfAbsent(dl.name(), dl);
            if (!added && !ifNotExists) {
                throw QueryException.error("Data connection already exists: " + dl.name());
            }
            if (!added) {
                // report only updates to listener
                listeners.forEach(TableListener::onTableChanged);
            }
            return added;
        }
    }

    public void removeDataConnection(String name, boolean ifExists) {
        if (!dataConnectionStorage.removeDataConnection(name)) {
            if (!ifExists) {
                throw QueryException.error("Data connection does not exist: " + name);
            }
        } else {
            listeners.forEach(TableListener::onTableChanged);
        }
    }

    /**
     * Invoke all registerer change listeners
     * TODO this method should be removed when the TODOs at calling sites are resolved
     */
    public void invokeChangeListeners() {
        listeners.forEach(TableListener::onTableChanged);
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

        ADDITIONAL_TABLE_PRODUCERS.forEach(producer -> tables.add(
                producer.apply(
                        getAllDataConnectionEntries(dataConnectionService, dataConnectionStorage),
                        connectorCache,
                        isSecurityEnabled
                )));
        return tables;
    }

    public static List<DataConnectionCatalogEntry> getAllDataConnectionEntries(
            DataConnectionServiceImpl dataConnectionService,
            DataConnectionStorage dataConnectionStorage) {
        // Collect config-originated data connections
        List<DataConnectionCatalogEntry> dataConnections =
                dataConnectionService.getConfigCreatedDataConnections()
                        .stream()
                        .map(dc -> new DataConnectionCatalogEntry(
                                dc.getName(),
                                dataConnectionService.typeForDataConnection(dc.getName()),
                                dc.getConfig().isShared(),
                                dc.options(),
                                DataConnectionSource.CONFIG))
                        .collect(toList());

        // And supplement them with data connections from sql catalog.
        // Note: __sql.catalog is the only source of truth for SQL-originated data connections.
        dataConnections.addAll(dataConnectionStorage.dataConnections());
        return dataConnections;
    }

    public static List<List<?>> getAllDataConnectionNameWithTypes(
            DataConnectionServiceImpl dataConnectionService) {
        List<DataConnection> conn = new ArrayList<>();
        conn.addAll(dataConnectionService.getConfigCreatedDataConnections());
        conn.addAll(dataConnectionService.getSqlCreatedDataConnections());

        return conn.stream()
                .map(dc -> asList(dc.getName(), dc.getConfig().getType(), jsonArray(dc.resourceTypes())))
                .collect(toList());
    }

    private static HazelcastJsonValue jsonArray(Collection<String> values) {
        return new HazelcastJsonValue(Json.array(values.toArray(new String[0])).toString());
    }

    @Override
    public void registerListener(TableListener listener) {
        listeners.add(listener);
    }
}
