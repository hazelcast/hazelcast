/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.mapstore.writethrough;

import com.hazelcast.map.impl.MapStoreWrapper;
import com.hazelcast.map.impl.mapstore.AbstractMapDataStore;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;

import java.util.Collection;
import java.util.Collections;

/**
 * Write through map data store implementation.
 * Created per map.
 */
public class WriteThroughStore extends AbstractMapDataStore<Data, Object> {

    public WriteThroughStore(MapStoreWrapper store, SerializationService serializationService) {
        super(store, serializationService);
    }

    @Override
    public Object add(Data key, Object value, long time) {
        Object objectValue = toObject(value);
        getStore().store(toObject(key), objectValue);
        // if store is not a post-processing map-store, then avoid extra de-serialization phase.
        return getStore().isPostProcessingMapStore() ? objectValue : value;
    }

    @Override
    public void addTransient(Data key, long now) {

    }

    @Override
    public Object addBackup(Data key, Object value, long time) {
        return value;
    }

    @Override
    public void remove(Data key, long time) {
        getStore().delete(toObject(key));

    }

    @Override
    public void removeBackup(Data key, long time) {

    }

    @Override
    public void reset() {

    }

    @Override
    public Object load(Data key) {
        return getStore().load(toObject(key));
    }

    @Override
    public boolean loadable(Data key, long lastUpdateTime, long now) {
        return true;
    }

    @Override
    public Collection<Data> flush() {
        return Collections.emptyList();
    }

    @Override
    public Object flush(Data key, Object value, long now, boolean backup) {
        return value;
    }

    @Override
    public int notFinishedOperationsCount() {
        return 0;
    }

}

