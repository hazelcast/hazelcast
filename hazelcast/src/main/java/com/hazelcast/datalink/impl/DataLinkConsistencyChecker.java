/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.datalink.impl;

import com.hazelcast.config.DataLinkConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.datalink.impl.DataLinkServiceImpl.DataLinkSource;
import com.hazelcast.datalink.impl.DataLinkServiceImpl.DataLinkSourcePair;
import com.hazelcast.jet.impl.JetServiceBackend;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.QueryUtils;
import com.hazelcast.sql.impl.schema.datalink.DataLink;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DataLinkConsistencyChecker {
    private final DataLinkServiceImpl dataLinkService;
    private final IMap<Object, Object> sqlCatalog;

    public DataLinkConsistencyChecker(HazelcastInstance instance, NodeEngine nodeEngine) {
        this.dataLinkService = (DataLinkServiceImpl) nodeEngine.getDataLinkService();
        this.sqlCatalog = instance.getMap(JetServiceBackend.SQL_CATALOG_MAP_NAME);
    }

    /**
     * Removes & alters all outdated data links in __sql.catalog
     * and adds all missed data links to __sql.catalog
     */
    public void check() {
        List<Map.Entry<String, DataLinkSourcePair>> sqlEntries = dataLinkService.getDataLinks()
                .entrySet()
                .stream()
                .filter(en -> en.getValue().source == DataLinkSource.SQL)
                .collect(Collectors.toList());

        for (Map.Entry<String, DataLinkSourcePair> entry : sqlEntries) {
            Object catalogValue = sqlCatalog.get(QueryUtils.wrapDataLinkKey(entry.getKey()));

            // Data link is absent in sql catalog -> add it.
            if (catalogValue == null) {
                addEntryToCatalog(entry);
            } else {
                // Try to alter outdated data links
                DataLink catalogDataLink = (DataLink) catalogValue;
                if (!dataLinksAreEqual(entry.getValue(), catalogDataLink)) {
                    sqlCatalog.remove(QueryUtils.wrapDataLinkKey(entry.getKey()));
                    addEntryToCatalog(entry);
                }
            }
        }

        // Data link is not present in service, but still present in sql.catalog
        sqlCatalog.removeAll(e -> e.getValue() instanceof DataLink
                && !dataLinkService.existsDataLink(((DataLink) e.getValue()).getName()));
    }

    // package-private for testing.
    void addEntryToCatalog(Map.Entry<String, DataLinkSourcePair> entry) {
        DataLinkSourcePair pair = entry.getValue();
        DataLink dataLink = new DataLink(
                pair.instance.getName(),
                pair.instance.getConfig().getClassName(),
                pair.instance.options());
        sqlCatalog.put(QueryUtils.wrapDataLinkKey(entry.getKey()), dataLink);
    }

    private boolean dataLinksAreEqual(DataLinkSourcePair pair, DataLink catalogDataLink) {
        DataLinkConfig catalogDLConfig = dataLinkService.toConfig(
                catalogDataLink.getName(),
                catalogDataLink.getType(),
                catalogDataLink.getOptions());

        return pair.instance.getConfig().equals(catalogDLConfig);
    }
}
