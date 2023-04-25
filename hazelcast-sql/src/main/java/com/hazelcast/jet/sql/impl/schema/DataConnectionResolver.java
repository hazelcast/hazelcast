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

import com.hazelcast.dataconnection.DataConnection;
import com.hazelcast.dataconnection.impl.DataConnectionServiceImpl;
import com.hazelcast.dataconnection.impl.DataConnectionServiceImpl.DataConnectionSource;
import com.hazelcast.dataconnection.impl.InternalDataConnectionService;
import com.hazelcast.internal.util.Preconditions;
import com.hazelcast.jet.sql.impl.connector.infoschema.DataConnectionsTable;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.schema.TableResolver;
import com.hazelcast.sql.impl.schema.dataconnection.DataConnectionCatalogEntry;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

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

    private static final List<Function<List<DataConnectionCatalogEntry>, Table>> ADDITIONAL_TABLE_PRODUCERS = singletonList(
            dl -> new DataConnectionsTable(CATALOG, SCHEMA_NAME_INFORMATION_SCHEMA, SCHEMA_NAME_PUBLIC, dl)
    );

    private final DataConnectionStorage dataConnectionStorage;
    private final DataConnectionServiceImpl dataConnectionService;

    public DataConnectionResolver(
            InternalDataConnectionService dataConnectionService,
            DataConnectionStorage dataConnectionStorage
    ) {
        Preconditions.checkInstanceOf(DataConnectionServiceImpl.class, dataConnectionService);
        this.dataConnectionService = (DataConnectionServiceImpl) dataConnectionService;
        this.dataConnectionStorage = dataConnectionStorage;
    }

    /**
     * @return true, if the data connection was created
     */
    public boolean createDataConnection(DataConnectionCatalogEntry dl, boolean replace, boolean ifNotExists) {
        if (replace) {
            dataConnectionStorage.put(dl.name(), dl);
            return true;
        } else {
            boolean added = dataConnectionStorage.putIfAbsent(dl.name(), dl);
            if (!added && !ifNotExists) {
                throw QueryException.error("Data connection already exists: " + dl.name());
            }
            return added;
        }
    }

    public void removeDataConnection(String name, boolean ifExists) {
        if (!dataConnectionStorage.removeDataConnection(name) && !ifExists) {
            throw QueryException.error("Data connection does not exist: " + name);
        }
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
                producer.apply(getAllDataConnectionEntries(dataConnectionService, dataConnectionStorage))
        ));
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
                   .map(dc -> Arrays.asList(dc.getName(), new ArrayList<>(dc.resourceTypes())))
                   .collect(toList());
    }

    @Override
    public void registerListener(TableListener listener) {
    }
}
