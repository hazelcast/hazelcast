/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.map.EntryLoader.MetadataAwareValue;
import com.hazelcast.map.impl.mapstore.AbstractMapDataStore;
import com.hazelcast.map.impl.mapstore.MapStoreContext;
import com.hazelcast.map.impl.mapstore.writebehind.entry.DelayedEntry;
import com.hazelcast.internal.serialization.Data;

import java.util.UUID;

/**
 * Write through map data store implementation.
 * Created per map.
 */
public class WriteThroughStore extends AbstractMapDataStore<Data, Object> {

    public WriteThroughStore(MapStoreContext mapStoreContext) {
        super(mapStoreContext);
    }

    @Override
    public Object add(Data key, Object value,
                      long expirationTime, long time, UUID transactionId) {
        Object objectKey = toObject(key);
        Object objectValue = toObject(value);

        if (getStore().isWithExpirationTime()) {
            expirationTime = getUserExpirationTime(expirationTime);
            getStore().store(objectKey, new MetadataAwareValue(objectValue, expirationTime));
        } else {
            getStore().store(objectKey, objectValue);
        }
        // if store is not a post-processing map-store, then avoid extra de-serialization phase.
        return getStore().isPostProcessingMapStore() ? objectValue : value;
    }

    @Override
    public void addForcibly(DelayedEntry delayedEntry) {
        throw new IllegalStateException("No addForcibly call is expected from a write-through store!");
    }

    @Override
    public void addTransient(Data key, long now) {

    }

    @Override
    public Object addBackup(Data key, Object value, long expirationTime,
                            long time, UUID transactionId) {
        return value;
    }

    @Override
    public void remove(Data key, long time, UUID transactionId) {
        getStore().delete(toObject(key));

    }

    @Override
    public void removeBackup(Data key, long time, UUID
            transactionId) {

    }

    @Override
    public void reset() {

    }

    @Override
    public Object load(Data key) {
        return getStore().load(toObject(key));
    }

    @Override
    public boolean loadable(Data key) {
        return true;
    }

    @Override
    public long softFlush() {
        // Only write-behind configured map-stores are flushable.
        return 0;
    }

    @Override
    public void hardFlush() {
        // Only write-behind configured map-stores are flushable.
    }

    @Override
    public Object flush(Data key, Object value, boolean backup) {
        return value;
    }

    @Override
    public int notFinishedOperationsCount() {
        return 0;
    }
}

