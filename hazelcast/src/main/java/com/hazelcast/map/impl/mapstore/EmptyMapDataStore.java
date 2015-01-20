package com.hazelcast.map.impl.mapstore;

import com.hazelcast.nio.serialization.Data;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 * Empty map data store for providing neutral null behaviour.
 */
class EmptyMapDataStore implements MapDataStore {

    @Override
    public Object add(Object key, Object value, long now) {
        return value;
    }

    @Override
    public void addTransient(Object key, long now) {

    }

    @Override
    public Object addBackup(Object key, Object value, long now) {
        return value;
    }

    @Override
    public void remove(Object key, long now) {

    }

    @Override
    public void removeBackup(Object key, long now) {

    }

    @Override
    public void clear() {

    }

    @Override
    public Object load(Object key) {
        return null;
    }

    @Override
    public Map loadAll(Collection keys) {
        return Collections.emptyMap();
    }

    @Override
    public void removeAll(Collection keys) {

    }

    @Override
    public boolean loadable(Object key, long lastUpdateTime, long now) {
        return false;
    }

    @Override
    public Collection<Data> flush() {
        return Collections.emptyList();
    }

    @Override
    public Object flush(Object key, Object value, long now, boolean backup) {
        return value;
    }

    @Override
    public int notFinishedOperationsCount() {
        return 0;
    }

    @Override
    public boolean isPostProcessingMapStore() {
        return false;
    }
}
