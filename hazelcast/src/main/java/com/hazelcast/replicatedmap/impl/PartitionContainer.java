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

package com.hazelcast.replicatedmap.impl;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.ReplicatedMapConfig;
import com.hazelcast.replicatedmap.impl.record.AbstractReplicatedRecordStore;
import com.hazelcast.replicatedmap.impl.record.DataReplicatedRecordStore;
import com.hazelcast.replicatedmap.impl.record.ObjectReplicatedRecordStorage;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecordStore;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;
import java.util.concurrent.ConcurrentHashMap;

public class PartitionContainer {

    private final ReplicatedMapService service;

    private final int partitionId;

    private final ConcurrentHashMap<String, ReplicatedRecordStore> replicatedStorages = initReplicatedRecordStoreMapping();

    private final ConstructorFunction<String, ReplicatedRecordStore> constructor = buildConstructorFunction();


    public PartitionContainer(ReplicatedMapService service, int partitionId) {
        this.service = service;
        this.partitionId = partitionId;
    }

    private ConcurrentHashMap<String, ReplicatedRecordStore> initReplicatedRecordStoreMapping() {
        return new ConcurrentHashMap<String, ReplicatedRecordStore>();
    }

    private ConstructorFunction<String, ReplicatedRecordStore> buildConstructorFunction() {
        return new ConstructorFunction<String, ReplicatedRecordStore>() {

            @Override
            public ReplicatedRecordStore createNew(String name) {
                ReplicatedMapConfig replicatedMapConfig = service.getReplicatedMapConfig(name);
                InMemoryFormat inMemoryFormat = replicatedMapConfig.getInMemoryFormat();
                AbstractReplicatedRecordStore replicatedRecordStorage = null;
                switch (inMemoryFormat) {
                    case OBJECT:
                        replicatedRecordStorage = new ObjectReplicatedRecordStorage(name, service);
                        break;
                    case BINARY:
                        replicatedRecordStorage = new DataReplicatedRecordStore(name, service);
                        break;
                    case NATIVE:
                        throw new IllegalStateException("Native memory not yet supported for replicated map");
                    default:
                        throw new IllegalStateException("Unhandled in memory format:" + inMemoryFormat);
                }
                return replicatedRecordStorage;
            }
        };
    }

    public ConcurrentHashMap<String, ReplicatedRecordStore> getStores() {
        return replicatedStorages;
    }

    public ReplicatedRecordStore getOrCreateRecordStore(String name) {
        ReplicatedRecordStore replicatedRecordStore = ConcurrencyUtil
                .getOrPutSynchronized(replicatedStorages, name, replicatedStorages, constructor);
        return replicatedRecordStore;
    }

    public ReplicatedRecordStore getRecordStore(String name) {
        return replicatedStorages.get(name);
    }

    public void shutdown() {
        for (ReplicatedRecordStore replicatedRecordStore : replicatedStorages.values()) {
            replicatedRecordStore.destroy();
        }
        replicatedStorages.clear();
    }

    public void destroy(String name) {
        ReplicatedRecordStore replicatedRecordStore = replicatedStorages.remove(name);
        if (replicatedRecordStore != null) {
            replicatedRecordStore.destroy();
        }

    }
}
