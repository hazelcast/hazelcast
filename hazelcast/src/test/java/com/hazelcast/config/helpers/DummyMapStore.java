package com.hazelcast.config.helpers;

import com.hazelcast.core.MapStore;
import java.util.Collection;
import java.util.Map;
import java.util.Set;


public class DummyMapStore implements MapStore<Object, Object> {

    public DummyMapStore(){}

    @Override
    public void store(Object key, Object value) {
    }

    @Override
    public void storeAll(Map<Object, Object> map) {
    }

    @Override
    public void delete(Object key) {
    }

    @Override
    public void deleteAll(Collection<Object> keys) {
    }

    @Override
    public Object load(Object key) {
        return null;
    }

    @Override
    public Map<Object, Object> loadAll(Collection<Object> keys) {
        return null;
    }

    @Override
    public Set<Object> loadAllKeys() {
        return null;
    }
}