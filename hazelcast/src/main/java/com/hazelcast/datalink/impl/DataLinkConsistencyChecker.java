package com.hazelcast.datalink.impl;

import com.hazelcast.datalink.impl.DataLinkServiceImpl.DataLinkSource;
import com.hazelcast.datalink.impl.DataLinkServiceImpl.DataLinkSourcePair;
import com.hazelcast.map.IMap;

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
     * Removes all outdated
     */
    public void check() {
        List<Map.Entry<String, DataLinkSourcePair>> sqlEntries = dataLinkService.getDataLinks()
                .entrySet()
                .stream()
                .filter(en -> en.getValue().source == DataLinkSource.SQL)
                .collect(Collectors.toList());

        for (Map.Entry<String, DataLinkSourcePair> entry : sqlEntries) {
//            if (sqlCatalog.get)
        }
    }
}
