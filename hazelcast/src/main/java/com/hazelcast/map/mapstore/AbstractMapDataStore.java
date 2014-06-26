package com.hazelcast.map.mapstore;

import com.hazelcast.map.MapStoreWrapper;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Abstract map data store contains common functionality of map data stores.
 *
 * @param <K> key type for map data store.
 * @param <V> value type for map data store.
 */
public abstract class AbstractMapDataStore<K, V> implements MapDataStore<K, V> {

    private MapStoreWrapper store;

    private SerializationService serializationService;

    protected AbstractMapDataStore(MapStoreWrapper store, SerializationService serializationService) {
        if (store == null || serializationService == null) {
            throw new NullPointerException();
        }
        this.store = store;
        this.serializationService = serializationService;
    }

    @Override
    public Map loadAll(Collection keys) {
        if (keys == null || keys.isEmpty()) {
            return Collections.emptyMap();
        }
        final List<Object> objectKeys = new ArrayList<Object>();
        for (Object key : keys) {
            objectKeys.add(toObject(key));
        }

        final Map entries = getStore().loadAll(objectKeys);
        if (entries == null || entries.isEmpty()) {
            return Collections.emptyMap();
        }
        return entries;
    }

    protected Object toObject(Object obj) {
        return serializationService.toObject(obj);
    }

    protected Data toData(Object obj) {
        return serializationService.toData(obj);
    }

    public MapStoreWrapper getStore() {
        return store;
    }
}
