/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.mapstore;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.map.impl.MapStoreWrapper;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Abstract map data store contains common functionality of map data stores.
 *
 * @param <K> key type for map data store
 * @param <V> value type for map data store
 */
public abstract class AbstractMapDataStore<K, V> implements MapDataStore<K, V> {

    private final MapStoreWrapper store;
    private final InternalSerializationService serializationService;

    protected AbstractMapDataStore(MapStoreWrapper store, InternalSerializationService serializationService) {
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
     * {@inheritDoc}
     *
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

    protected Data toHeapData(Object obj) {
        return serializationService.toData(obj, DataType.HEAP);
    }

    public MapStoreWrapper getStore() {
        return store;
    }

    /**
     * Deserialises all of the items in the provided collection if they
     * are not deserialised already.
     *
     * @param keys the items to be deserialised
     * @return the list of deserialised items
     */
    private List<Object> convertToObjectKeys(Collection keys) {
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
