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

package com.hazelcast.sql.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.datalink.impl.DataLinkServiceImpl;
import com.hazelcast.datalink.impl.DataLinkServiceImpl.DataLinkEntry;
import com.hazelcast.datalink.impl.DataLinkServiceImpl.DataLinkSource;
import com.hazelcast.jet.impl.JetServiceBackend;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.schema.datalink.DataLinkCatalogEntry;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DataLinkConsistencyChecker {
    private final HazelcastInstance hazelcastInstance;
    private final DataLinkServiceImpl dataLinkService;
    // sqlCatalog supposed to be initialized lazily
    private IMap<Object, Object> sqlCatalog;

    private boolean initialized;

    public DataLinkConsistencyChecker(HazelcastInstance instance, NodeEngine nodeEngine) {
        this.hazelcastInstance = instance;
        this.dataLinkService = (DataLinkServiceImpl) nodeEngine.getDataLinkService();
    }

    /**
     * Lazily initialize sqlCatalog.
     * The only reason of this method existing is
     * to prevent eager creation of SQL catalog IMap.
     */
    public void init() {
        sqlCatalog = hazelcastInstance.getMap(JetServiceBackend.SQL_CATALOG_MAP_NAME);
        initialized = true;
    }

    /**
     * Removes & alters all outdated data links in __sql.catalog
     * and adds all missed data links to __sql.catalog
     */
    public void check() {
        if (!initialized) {
            return;
        }

        List<Map.Entry<String, DataLinkEntry>> sqlEntries = dataLinkService.getDataLinks()
                .entrySet()
                .stream()
                .filter(en -> en.getValue().getSource() == DataLinkSource.SQL)
                .collect(Collectors.toList());

        for (Object catalogItem : sqlCatalog.values()) {
            if (!(catalogItem instanceof DataLinkCatalogEntry)) {
                continue;
            }
            DataLinkCatalogEntry dl = (DataLinkCatalogEntry) catalogItem;
            dataLinkService.replaceSqlDataLink(dl.getName(), dl.getType(), dl.getOptions());
        }

        for (Map.Entry<String, DataLinkEntry> entry : sqlEntries) {
            Object catalogValue = sqlCatalog.get(QueryUtils.wrapDataLinkKey(entry.getKey()));

            // Data link is absent in sql catalog -> remove it from data link service.
            if (catalogValue == null) {
                dataLinkService.removeDataLink(entry.getKey());
            }
        }
    }

    public boolean isInitialized() {
        return initialized;
    }

}
