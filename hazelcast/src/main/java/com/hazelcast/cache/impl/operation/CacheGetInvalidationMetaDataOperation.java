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

import com.hazelcast.cache.impl.CacheDataSerializerHook;
import com.hazelcast.cache.impl.CacheEventHandler;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.internal.nearcache.impl.invalidation.MetaDataGenerator;
import com.hazelcast.cluster.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.ReadonlyOperation;
import com.hazelcast.internal.partition.IPartitionService;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.hazelcast.cache.impl.CacheDataSerializerHook.CACHE_INVALIDATION_METADATA;
import static com.hazelcast.cache.impl.CacheDataSerializerHook.CACHE_INVALIDATION_METADATA_RESPONSE;
import static com.hazelcast.internal.util.CollectionUtil.isNotEmpty;
import static com.hazelcast.internal.util.Preconditions.checkTrue;

public class CacheGetInvalidationMetaDataOperation extends Operation implements IdentifiedDataSerializable, ReadonlyOperation {

    private List<String> names;
    private MetaDataResponse response;

    public CacheGetInvalidationMetaDataOperation() {
    }

    public CacheGetInvalidationMetaDataOperation(List<String> names) {
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
        response.partitionUuidList = getPartitionUuidList(ownedPartitions);
        response.namePartitionSequenceList = getNamePartitionSequenceList(ownedPartitions);
    }

    public static class MetaDataResponse implements IdentifiedDataSerializable {

        /**
         * map of map-name, partition to sequence mapping list
         */
        private Map<String, List<Map.Entry<Integer, Long>>> namePartitionSequenceList;

        /**
         * map of partition ID and UUID
         */
        private Map<Integer, UUID> partitionUuidList;

        public Map<String, List<Map.Entry<Integer, Long>>> getNamePartitionSequenceList() {
            return namePartitionSequenceList;
        }

        public Map<Integer, UUID> getPartitionUuidList() {
            return partitionUuidList;
        }

        @Override
        public int getFactoryId() {
            return CacheDataSerializerHook.F_ID;
        }

        @Override
        public int getClassId() {
            return CACHE_INVALIDATION_METADATA_RESPONSE;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeInt(namePartitionSequenceList.size());
            for (Map.Entry<String, List<Map.Entry<Integer, Long>>> entry : namePartitionSequenceList.entrySet()) {
                out.writeString(entry.getKey());
                out.writeInt(entry.getValue().size());
                for (Map.Entry<Integer, Long> seqEntry : entry.getValue()) {
                    out.writeInt(seqEntry.getKey());
                    out.writeLong(seqEntry.getValue());
                }
            }

            out.writeInt(partitionUuidList.size());
            for (Map.Entry<Integer, UUID> entry : partitionUuidList.entrySet()) {
                out.writeInt(entry.getKey());
                out.writeLong(entry.getValue().getMostSignificantBits());
                out.writeLong(entry.getValue().getLeastSignificantBits());
            }
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            int size1 = in.readInt();
            namePartitionSequenceList = new HashMap<String, List<Map.Entry<Integer, Long>>>(size1);
            for (int i = 0; i < size1; i++) {
                String name = in.readString();
                int size2 = in.readInt();
                List<Map.Entry<Integer, Long>> innerList = new ArrayList<Map.Entry<Integer, Long>>(size2);
                for (int j = 0; j < size2; j++) {
                    int partition = in.readInt();
                    long seq = in.readLong();
                    innerList.add(new AbstractMap.SimpleEntry<Integer, Long>(partition, seq));
                }
                namePartitionSequenceList.put(name, innerList);
            }

            int size3 = in.readInt();
            partitionUuidList = new HashMap<Integer, UUID>(size3);
            for (int i = 0; i < size3; i++) {
                int partition = in.readInt();
                UUID uuid = new UUID(in.readLong(), in.readLong());
                partitionUuidList.put(partition, uuid);
            }
        }
    }

    private List<Integer> getOwnedPartitions() {
        IPartitionService partitionService = getNodeEngine().getPartitionService();
        Map<Address, List<Integer>> memberPartitionsMap = partitionService.getMemberPartitionsMap();
        List<Integer> ownedPartitions = memberPartitionsMap.get(getNodeEngine().getThisAddress());
        return ownedPartitions == null ? Collections.<Integer>emptyList() : ownedPartitions;
    }

    private Map<Integer, UUID> getPartitionUuidList(List<Integer> ownedPartitionIds) {
        MetaDataGenerator metaDataGenerator = getPartitionMetaDataGenerator();
        Map<Integer, UUID> partitionUuids = new HashMap<Integer, UUID>(ownedPartitionIds.size());
        for (Integer partitionId : ownedPartitionIds) {
            UUID uuid = metaDataGenerator.getOrCreateUuid(partitionId);
            partitionUuids.put(partitionId, uuid);
        }
        return partitionUuids;
    }

    private Map<String, List<Map.Entry<Integer, Long>>> getNamePartitionSequenceList(List<Integer> ownedPartitionIds) {
        MetaDataGenerator metaDataGenerator = getPartitionMetaDataGenerator();
        Map<String, List<Map.Entry<Integer, Long>>> sequences = new HashMap<String, List<Map.Entry<Integer, Long>>>(
                ownedPartitionIds.size());

        for (String name : names) {
            List<Map.Entry<Integer, Long>> mapSequences = new ArrayList<Map.Entry<Integer, Long>>();
            for (Integer partitionId : ownedPartitionIds) {
                long partitionSequence = metaDataGenerator.currentSequence(name, partitionId);
                if (partitionSequence != 0) {
                    mapSequences.add(new AbstractMap.SimpleEntry<Integer, Long>(partitionId, partitionSequence));
                }
            }
            sequences.put(name, mapSequences);
        }
        return sequences;
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
            out.writeString(mapName);
        }
    }

    @Override
    public void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);

        int size = in.readInt();

        List<String> mapNames = new ArrayList<String>(size);
        for (int i = 0; i < size; i++) {
            mapNames.add(in.readString());
        }

        this.names = mapNames;
    }

    @Override
    public int getFactoryId() {
        return CacheDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return CACHE_INVALIDATION_METADATA;
    }

}
