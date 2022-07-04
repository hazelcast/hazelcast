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

package com.hazelcast.cache.impl.operation;

import com.hazelcast.cache.impl.AbstractCacheService;
import com.hazelcast.cache.impl.CacheDataSerializerHook;
import com.hazelcast.cache.impl.CacheEventHandler;
import com.hazelcast.cache.impl.CachePartitionSegment;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.internal.nearcache.impl.invalidation.MetaDataGenerator;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.internal.services.ObjectNamespace;
import com.hazelcast.internal.services.ServiceNamespace;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static java.util.Collections.emptyList;

/**
 * Holder for Near Cache metadata.
 */
public class CacheNearCacheStateHolder implements IdentifiedDataSerializable {

    private volatile UUID partitionUuid;
    private volatile List<Object> cacheNameSequencePairs = emptyList();
    private Operation cacheReplicationOperation;

    public CacheNearCacheStateHolder() {
    }

    public void setCacheReplicationOperation(Operation cacheReplicationOperation) {
        this.cacheReplicationOperation = cacheReplicationOperation;
    }

    void prepare(CachePartitionSegment segment, Collection<ServiceNamespace> namespaces) {
        ICacheService cacheService = segment.getCacheService();
        MetaDataGenerator metaData = getPartitionMetaDataGenerator(cacheService);

        int partitionId = segment.getPartitionId();
        partitionUuid = metaData.getOrCreateUuid(partitionId);

        cacheNameSequencePairs = Collections.synchronizedList(new ArrayList(namespaces.size()));
        for (ServiceNamespace namespace : namespaces) {
            ObjectNamespace ns = (ObjectNamespace) namespace;
            String cacheName = ns.getObjectName();

            cacheNameSequencePairs.add(cacheName);
            cacheNameSequencePairs.add(metaData.currentSequence(cacheName, partitionId));
        }
    }

    private MetaDataGenerator getPartitionMetaDataGenerator(ICacheService cacheService) {
        CacheEventHandler cacheEventHandler = ((AbstractCacheService) cacheService).getCacheEventHandler();
        return cacheEventHandler.getMetaDataGenerator();
    }

    public void applyState() {
        CacheService cacheService = cacheReplicationOperation.getService();
        MetaDataGenerator metaDataGenerator = getPartitionMetaDataGenerator(cacheService);

        int partitionId = cacheReplicationOperation.getPartitionId();

        if (partitionUuid != null) {
            metaDataGenerator.setUuid(partitionId, partitionUuid);
        }

        for (int i = 0; i < cacheNameSequencePairs.size(); ) {
            String cacheName = (String) cacheNameSequencePairs.get(i++);
            long sequence = (Long) cacheNameSequencePairs.get(i++);

            metaDataGenerator.setCurrentSequence(cacheName, partitionId, sequence);
        }
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        boolean nullUuid = partitionUuid == null;
        out.writeBoolean(nullUuid);
        if (!nullUuid) {
            out.writeLong(partitionUuid.getMostSignificantBits());
            out.writeLong(partitionUuid.getLeastSignificantBits());
        }

        out.writeInt(cacheNameSequencePairs.size());
        for (Object item : cacheNameSequencePairs) {
            out.writeObject(item);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        boolean nullUuid = in.readBoolean();
        partitionUuid = nullUuid ? null : new UUID(in.readLong(), in.readLong());

        int size = in.readInt();
        cacheNameSequencePairs = Collections.synchronizedList(new ArrayList(size));
        for (int i = 0; i < size; i++) {
            cacheNameSequencePairs.add(in.readObject());
        }
    }

    @Override
    public int getFactoryId() {
        return CacheDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return CacheDataSerializerHook.CACHE_NEAR_CACHE_STATE_HOLDER;
    }
}
