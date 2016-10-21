package com.hazelcast.spring;

import com.hazelcast.core.QueueStore;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class DummyQueueStore implements QueueStore {
    @Override
    public void store(Long key, Object value) {

    }

    @Override
    public void storeAll(Map map) {

    }

    @Override
    public void delete(Long key) {

    }

    @Override
    public Object load(Long key) {
        return null;
    }

    @Override
    public Set<Long> loadAllKeys() {
        return null;
    }

    @Override
    public Map loadAll(Collection keys) {
        return null;
    }

    @Override
    public void deleteAll(Collection keys) {

    }
}
