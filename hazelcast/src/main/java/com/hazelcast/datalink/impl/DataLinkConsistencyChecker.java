package com.hazelcast.datalink.impl;

import com.hazelcast.config.DataLinkConfig;
import com.hazelcast.datalink.impl.DataLinkServiceImpl.DataLinkSource;
import com.hazelcast.datalink.impl.DataLinkServiceImpl.DataLinkSourcePair;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.impl.QueryUtils;
import com.hazelcast.sql.impl.schema.datalink.DataLink;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DataLinkConsistencyChecker {
    private final DataLinkServiceImpl dataLinkService;
    private final IMap<Object, Object> sqlCatalog;

    public DataLinkConsistencyChecker(InternalDataLinkService dataLinkService, IMap<Object, Object> sqlCatalog) {
        assert dataLinkService instanceof DataLinkServiceImpl;
        this.dataLinkService = (DataLinkServiceImpl) dataLinkService;
        this.sqlCatalog = sqlCatalog;
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
                assert catalogValue instanceof DataLink;
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

    boolean dataLinksAreEqual(DataLinkSourcePair pair, DataLink catalogDataLink) {
        DataLinkConfig catalogDLConfig = dataLinkService.toConfig(
                catalogDataLink.getName(),
                catalogDataLink.getType(),
                catalogDataLink.getOptions());

        return pair.instance.getConfig().equals(catalogDLConfig);
    }
}
