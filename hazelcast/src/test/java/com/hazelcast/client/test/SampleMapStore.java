package com.hazelcast.client.test;

import com.hazelcast.core.MapStore;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * This class is for Non-java clients. Please do not remove or modify.
 */
public class SampleMapStore implements MapStore<String, String> {
    private ConcurrentMap<String, String> internalStore = new ConcurrentHashMap<String, String>();

    @Override
    public void store(String key, String value) {
        internalStore.put(key, value);
    }

    @Override
    public void storeAll(Map<String, String> map) {
        internalStore.putAll(map);
    }

    @Override
    public void delete(String key) {
        internalStore.remove(key);
    }

    @Override
    public void deleteAll(Collection<String> keys) {
        for(String key : keys) {
            delete(key);
        }
    }

    @Override
    public String load(String key) {
        return internalStore.get(key);
    }

    @Override
    public Map<String, String> loadAll(Collection<String> keys) {
        HashMap<String, String> resultMap = new HashMap<String, String>();
        for (String key: keys) {
            resultMap.put(key, internalStore.get(key));
        }
        return resultMap;
    }

    @Override
    public Iterable<String> loadAllKeys() {
        return internalStore.keySet();
    }
}
