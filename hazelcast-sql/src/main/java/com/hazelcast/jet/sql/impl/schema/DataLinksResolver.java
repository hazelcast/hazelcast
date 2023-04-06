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

import com.hazelcast.datalink.impl.DataLinkServiceImpl;
import com.hazelcast.datalink.impl.DataLinkServiceImpl.DataLinkSource;
import com.hazelcast.datalink.impl.InternalDataLinkService;
import com.hazelcast.internal.util.Preconditions;
import com.hazelcast.jet.sql.impl.connector.infoschema.DataLinksTable;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.schema.TableResolver;
import com.hazelcast.sql.impl.schema.datalink.DataLinkCatalogEntry;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.hazelcast.sql.impl.QueryUtils.CATALOG;
import static com.hazelcast.sql.impl.QueryUtils.SCHEMA_NAME_INFORMATION_SCHEMA;
import static com.hazelcast.sql.impl.QueryUtils.SCHEMA_NAME_PUBLIC;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

public class DataLinksResolver implements TableResolver {
    // It will be in a separate schema, so separate resolver is implemented.
    private static final List<List<String>> SEARCH_PATHS = singletonList(
            asList(CATALOG, SCHEMA_NAME_PUBLIC)
    );

    private static final List<Function<List<DataLinkCatalogEntry>, Table>> ADDITIONAL_TABLE_PRODUCERS = singletonList(
            dl -> new DataLinksTable(CATALOG, SCHEMA_NAME_INFORMATION_SCHEMA, SCHEMA_NAME_PUBLIC, dl)
    );

    private final DataLinkStorage dataLinkStorage;
    private final DataLinkServiceImpl dataLinkService;

    public DataLinksResolver(InternalDataLinkService dataLinkService, DataLinkStorage dataLinkStorage) {
        Preconditions.checkInstanceOf(DataLinkServiceImpl.class, dataLinkService);
        this.dataLinkService = (DataLinkServiceImpl) dataLinkService;
        this.dataLinkStorage = dataLinkStorage;
    }

    /**
     * @return true, if the datalink was created
     */
    public boolean createDataLink(DataLinkCatalogEntry dl, boolean replace, boolean ifNotExists) {
        if (replace) {
            dataLinkStorage.put(dl.name(), dl);
            return true;
        } else {
            boolean added = dataLinkStorage.putIfAbsent(dl.name(), dl);
            if (!added && !ifNotExists) {
                throw QueryException.error("Data link already exists: " + dl.name());
            }
            return added;
        }
    }

    public void removeDataLink(String name, boolean ifExists) {
        if (!dataLinkStorage.removeDataLink(name) && !ifExists) {
            throw QueryException.error("Data link does not exist: " + name);
        }
    }

    public DataLinkStorage getDataLinkStorage() {
        return dataLinkStorage;
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

        ADDITIONAL_TABLE_PRODUCERS.forEach(
                producer -> tables.add(producer.apply(getAllDataLinkEntries(dataLinkService, dataLinkStorage))));
        return tables;
    }

    public static List<DataLinkCatalogEntry> getAllDataLinkEntries(
            DataLinkServiceImpl dataLinkService,
            DataLinkStorage dataLinkStorage) {
        // Collect config-originated data links
        List<DataLinkCatalogEntry> dataLinks = dataLinkService.getConfigCreatedDataLinks()
                .stream()
                .map(dl -> new DataLinkCatalogEntry(
                        dl.getName(),
                        dataLinkService.typeForDataLink(dl.getName()),
                        dl.getConfig().isShared(),
                        dl.options(),
                        DataLinkSource.CONFIG))
                .collect(Collectors.toList());

        // And supplement them with data links from sql catalog.
        // Note: __sql.catalog is the only source of truth for SQL-originated data links.
        dataLinks.addAll(dataLinkStorage.dataLinks());
        return dataLinks;
    }

    @Override
    public void registerListener(TableListener listener) {
    }
}
