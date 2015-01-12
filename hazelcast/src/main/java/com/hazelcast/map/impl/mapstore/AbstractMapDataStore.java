package com.hazelcast.map.impl.mapstore;

import com.hazelcast.map.impl.MapStoreWrapper;
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
        final List<Object> objectKeys = convertToObjectKeys(keys);

        final Map entries = getStore().loadAll(objectKeys);

        if (entries == null || entries.isEmpty()) {
            return Collections.emptyMap();
        }
        return entries;
    }

    /**
     * Directly removes keys from map store as in write-through mode.
     * It works same for write-behind and write-through stores.
     */
    @Override
    public void removeAll(Collection keys) {
        if (keys == null || keys.isEmpty()) {
            return;
        }
        final List<Object> objectKeys = convertToObjectKeys(keys);
        getStore().deleteAll(objectKeys);
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

    protected List<Object> convertToObjectKeys(Collection keys) {
        if (keys == null || keys.isEmpty()) {
            return Collections.emptyList();
        }
        final List<Object> objectKeys = new ArrayList<Object>(keys.size());
        for (Object key : keys) {
            objectKeys.add(toObject(key));
        }
        return objectKeys;
    }

    @Override
    public boolean isPostProcessingMapStore() {
        return store.isPostProcessingMapStore();
    }
}
