/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.hibernate.distributed;

import com.hazelcast.hibernate.serialization.Expirable;
import com.hazelcast.hibernate.serialization.ExpiryMarker;
import com.hazelcast.hibernate.serialization.HibernateDataSerializerHook;
import com.hazelcast.hibernate.serialization.Value;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.Map;

/**
 * A concrete implementation of {@link com.hazelcast.map.EntryProcessor} which attempts
 * to update a region cache entry
 */
public class UpdateEntryProcessor extends AbstractRegionCacheEntryProcessor {

    private ExpiryMarker lock;
    private Object newValue;
    private Object newVersion;
    private String nextMarkerId;
    private long timestamp;

    public UpdateEntryProcessor() {
    }

    public UpdateEntryProcessor(ExpiryMarker lock, Object newValue, Object newVersion, String nextMarkerId, long timestamp) {
        this.lock = lock;
        this.nextMarkerId = nextMarkerId;
        this.newValue = newValue;
        this.newVersion = newVersion;
        this.timestamp = timestamp;
    }

    @Override
    public Boolean process(Map.Entry<Object, Expirable> entry) {
        Expirable expirable = entry.getValue();
        boolean updated;

        if (expirable == null) {
            // Nothing there. The entry was evicted? It should be safe to replace it
            expirable = new Value(newVersion, timestamp, newValue);
            updated = true;
        } else {
            if (expirable.matches(lock)) {
                final ExpiryMarker marker = (ExpiryMarker) expirable;
                if (marker.isConcurrent()) {
                    // Multiple transactions are attempting to update the same entry. Its highly
                    // likely that the value we are attempting to set is invalid. Instead just
                    // expire the entry and allow the next put to the cache to succeed if no more
                    // transactions are in-flight.
                    expirable = marker.expire(timestamp);
                    updated = false;
                } else {
                    // Only one transaction attempted to update the entry so it is safe to replace
                    // it with the value supplied
                    expirable = new Value(newVersion, timestamp, newValue);
                    updated = true;
                }
            } else if (expirable.getValue() == null) {
                // It's a different marker, Leave it as is
                return false;
            } else {
                // It's a value. We have no way to see which is correct so we expire the entry.
                // It is expired instead of removed to prevent in progress transactions from
                // putting stale values into the cache
                expirable = new ExpiryMarker(newVersion, timestamp, nextMarkerId).expire(timestamp);
                updated = false;
            }
        }

        entry.setValue(expirable);
        return updated;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(lock);
        out.writeObject(newValue);
        out.writeObject(newVersion);
        out.writeUTF(nextMarkerId);
        out.writeLong(timestamp);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        lock = in.readObject();
        newValue = in.readObject();
        newVersion = in.readObject();
        nextMarkerId = in.readUTF();
        timestamp = in.readLong();
    }

    @Override
    public int getId() {
        return HibernateDataSerializerHook.UPDATE;
    }

}
