package com.hazelcast.map;

import com.hazelcast.monitor.impl.LocalMapStatsImpl;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

abstract class LocalMapStatsFactory {

    private final ConcurrentMap<String, LocalMapStatsImpl> statsMap
            = new ConcurrentHashMap<String, LocalMapStatsImpl>(1000);

    private final ConstructorFunction<String, LocalMapStatsImpl> localMapStatsConstructorFunction
            = new ConstructorFunction<String, LocalMapStatsImpl>() {
        public LocalMapStatsImpl createNew(String key) {
            return new LocalMapStatsImpl();
        }
    };

    public LocalMapStatsImpl getLocalMapStatsImpl(String name) {
        return ConcurrencyUtil.getOrPutIfAbsent(statsMap, name, localMapStatsConstructorFunction);
    }

}
