/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cache.impl.CacheDataSerializerHook;
import com.hazelcast.cache.impl.CacheEventHandler;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.internal.nearcache.impl.invalidation.MetaDataGenerator;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.ReadonlyOperation;
import com.hazelcast.spi.partition.IPartitionService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static com.hazelcast.cache.impl.CacheDataSerializerHook.CACHE_INVALIDATION_METADATA;
import static com.hazelcast.cache.impl.CacheDataSerializerHook.CACHE_INVALIDATION_METADATA_RESPONSE;
import static com.hazelcast.util.CollectionUtil.isNotEmpty;
import static com.hazelcast.util.Preconditions.checkTrue;

public class GetInvalidationMetaDataOperation extends Operation implements IdentifiedDataSerializable, ReadonlyOperation {

    private List<String> names;
    private MetaDataResponse response;

    public GetInvalidationMetaDataOperation() {
    }

    public GetInvalidationMetaDataOperation(List<String> names) {
        checkTrue(isNotEmpty(names), "names cannot be null or empty");
        this.names = names;
    }

    @Override
    public String getServiceName() {
        return CacheService.SERVICE_NAME;
    }

    @Override
    public void run() {
        List<Integer> ownedPartitions = getOwnedPartitions();

        response = new MetaDataResponse();
        response.uuids = getUuidsOf(ownedPartitions);
        response.sequences = getSequencesOf(ownedPartitions);
    }

    public static class MetaDataResponse implements IdentifiedDataSerializable {
        private List<Object> sequences;
        private List<Object> uuids;

        public List<Object> getSequences() {
            return sequences;
        }

        public List<Object> getUuids() {
            return uuids;
        }

        @Override
        public int getFactoryId() {
            return CacheDataSerializerHook.F_ID;
        }

        @Override
        public int getId() {
            return CACHE_INVALIDATION_METADATA_RESPONSE;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeInt(sequences.size());
            for (Object o : sequences) {
                out.writeObject(o);
            }

            out.writeInt(uuids.size());
            for (Object o : uuids) {
                out.writeObject(o);
            }
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            int size1 = in.readInt();
            sequences = new ArrayList(size1);
            for (int i = 0; i < size1; i++) {
                sequences.add(in.readObject());
            }

            int size2 = in.readInt();
            uuids = new ArrayList(size2);
            for (int i = 0; i < size2; i++) {
                uuids.add(in.readObject());
            }
        }
    }

    private List<Object> getSequencesOf(List<Integer> ownedPartitionIds) {
        MetaDataGenerator metaDataGenerator = getPartitionMetaDataGenerator();
        List<Object> mapSequences = new ArrayList(ownedPartitionIds.size() * 2);

        for (String mapName : names) {
            mapSequences.add(mapName);

            for (Integer partitionId : ownedPartitionIds) {
                long partitionSequence = metaDataGenerator.currentSequence(mapName, partitionId);
                if (partitionSequence != 0) {
                    mapSequences.add(partitionId);
                    mapSequences.add(partitionSequence);
                }
            }
        }

        return mapSequences;
    }

    private List<Object> getUuidsOf(List<Integer> ownedPartitionIds) {
        MetaDataGenerator metaDataGenerator = getPartitionMetaDataGenerator();

        List<Object> partitionUUIDs = new ArrayList(ownedPartitionIds.size() * 2);
        for (Integer partitionId : ownedPartitionIds) {
            UUID uuid = metaDataGenerator.getUuidOrNull(partitionId);
            if (uuid != null) {
                partitionUUIDs.add(partitionId);
                partitionUUIDs.add(uuid.getMostSignificantBits());
                partitionUUIDs.add(uuid.getLeastSignificantBits());
            }
        }

        return partitionUUIDs;
    }

    private List<Integer> getOwnedPartitions() {
        List<Integer> ownedPartitions = new ArrayList<Integer>();
        IPartitionService partitionService = getNodeEngine().getPartitionService();
        for (int i = 0; i < partitionService.getPartitionCount(); i++) {
            if (partitionService.isPartitionOwner(i)) {
                ownedPartitions.add(i);
            }
        }
        return ownedPartitions;
    }

    private MetaDataGenerator getPartitionMetaDataGenerator() {
        CacheService cacheService = getService();
        CacheEventHandler cacheEventHandler = cacheService.getCacheEventHandler();
        return cacheEventHandler.getMetaDataGenerator();
    }

    @Override
    public Object getResponse() {
        return response;
    }

    @Override
    public void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);

        out.writeInt(names.size());

        for (String mapName : names) {
            out.writeUTF(mapName);
        }
    }

    @Override
    public void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);

        int size = in.readInt();

        List<String> mapNames = new ArrayList<String>(size);
        for (int i = 0; i < size; i++) {
            mapNames.add(in.readUTF());
        }

        this.names = mapNames;
    }

    @Override
    public int getFactoryId() {
        return CacheDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return CACHE_INVALIDATION_METADATA;
    }

}
