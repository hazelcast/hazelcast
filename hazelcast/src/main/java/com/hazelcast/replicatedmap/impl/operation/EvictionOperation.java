/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.replicatedmap.impl.operation;

import com.hazelcast.replicatedmap.impl.record.ReplicatedRecordStore;
import com.hazelcast.spi.impl.MutatingOperation;
import com.hazelcast.util.scheduler.ScheduledEntry;

import java.util.Collection;

/**
 * Evicts set of entries from the record store. Runs locally.
 */
public class EvictionOperation extends AbstractNamedSerializableOperation implements MutatingOperation {

    private ReplicatedRecordStore store;
    private Collection<ScheduledEntry<Object, Object>> entries;

    public EvictionOperation() {
    }

    public EvictionOperation(ReplicatedRecordStore store, Collection<ScheduledEntry<Object, Object>> entries) {
        this.store = store;
        this.entries = entries;
    }

    @Override
    public void run() throws Exception {
        for (ScheduledEntry<Object, Object> entry : entries) {
            Object key = entry.getKey();
            store.evict(key);
        }
    }

    @Override
    public boolean validatesTarget() {
        return false;
    }

    @Override
    public int getId() {
        return ReplicatedMapDataSerializerHook.EVICTION;
    }

    @Override
    public String getName() {
        return store.getName();
    }
}
