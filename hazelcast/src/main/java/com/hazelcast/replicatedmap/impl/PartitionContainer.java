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

package com.hazelcast.replicatedmap.impl;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.ReplicatedMapConfig;
import com.hazelcast.replicatedmap.impl.record.DataReplicatedRecordStore;
import com.hazelcast.replicatedmap.impl.record.ObjectReplicatedRecordStorage;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecordStore;
import com.hazelcast.internal.util.ConstructorFunction;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.internal.util.ConcurrencyUtil.getOrPutSynchronized;

/**
 * Contains the record storage for the replicated maps which are the actual place where the data is stored.
 */
public class PartitionContainer {

    private final ConcurrentHashMap<String, ReplicatedRecordStore> replicatedRecordStores = initReplicatedRecordStoreMapping();
    private final ConstructorFunction<String, ReplicatedRecordStore> constructor = buildConstructorFunction();

    private final ReplicatedMapService service;
    private final int partitionId;

    public PartitionContainer(ReplicatedMapService service, int partitionId) {
        this.service = service;
        this.partitionId = partitionId;
    }

    private ConcurrentHashMap<String, ReplicatedRecordStore> initReplicatedRecordStoreMapping() {
        return new ConcurrentHashMap<>();
    }

    private ConstructorFunction<String, ReplicatedRecordStore> buildConstructorFunction() {
        return name -> {
            ReplicatedMapConfig replicatedMapConfig = service.getReplicatedMapConfig(name);
            InMemoryFormat inMemoryFormat = replicatedMapConfig.getInMemoryFormat();
            switch (inMemoryFormat) {
                case OBJECT:
                    return new ObjectReplicatedRecordStorage(name, service, partitionId);
                case BINARY:
                    return new DataReplicatedRecordStore(name, service, partitionId);
                case NATIVE:
                    throw new IllegalStateException("Native memory not yet supported for replicated map");
                default:
                    throw new IllegalStateException("Unsupported in memory format: " + inMemoryFormat);
            }
        };
    }

    public boolean isEmpty() {
        return replicatedRecordStores.isEmpty();
    }

    public ConcurrentMap<String, ReplicatedRecordStore> getStores() {
        return replicatedRecordStores;
    }

    public ReplicatedRecordStore getOrCreateRecordStore(String name) {
        return getOrPutSynchronized(replicatedRecordStores, name, replicatedRecordStores, constructor);
    }

    public ReplicatedRecordStore getRecordStore(String name) {
        return replicatedRecordStores.get(name);
    }

    public void shutdown() {
        for (ReplicatedRecordStore replicatedRecordStore : replicatedRecordStores.values()) {
            replicatedRecordStore.destroy();
        }
        replicatedRecordStores.clear();
    }

    public void destroy(String name) {
        ReplicatedRecordStore replicatedRecordStore = replicatedRecordStores.remove(name);
        if (replicatedRecordStore != null) {
            replicatedRecordStore.destroy();
        }
    }
}
