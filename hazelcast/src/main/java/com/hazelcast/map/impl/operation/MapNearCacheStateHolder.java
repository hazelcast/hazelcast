/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.operation;

import com.hazelcast.internal.nearcache.impl.invalidation.Invalidator;
import com.hazelcast.internal.nearcache.impl.invalidation.MetaDataGenerator;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.nearcache.MapNearCacheManager;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.internal.services.ObjectNamespace;
import com.hazelcast.internal.services.ServiceNamespace;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import static com.hazelcast.map.impl.MapDataSerializerHook.MAP_NEAR_CACHE_STATE_HOLDER;
import static java.util.Collections.emptyList;

/**
 * Holder for Near Cache metadata.
 */
public class MapNearCacheStateHolder implements IdentifiedDataSerializable {

    // keep this `protected`, extended in another context
    protected UUID partitionUuid;
    protected List<Object> mapNameSequencePairs = emptyList();

    private MapReplicationOperation mapReplicationOperation;

    /**
     * This constructor exists solely for instantiation by {@code MapDataSerializerHook}. The object is not ready to use
     * unless {@code mapReplicationOperation} is set.
     */
    public MapNearCacheStateHolder() {
    }

    public MapNearCacheStateHolder(MapReplicationOperation mapReplicationOperation) {
        this.mapReplicationOperation = mapReplicationOperation;
    }

    void prepare(PartitionContainer container, Collection<ServiceNamespace> namespaces, int replicaIndex) {
        MapService mapService = container.getMapService();

        MetaDataGenerator metaData = getPartitionMetaDataGenerator(mapService);

        int partitionId = container.getPartitionId();
        partitionUuid = metaData.getOrCreateUuid(partitionId);

        for (ServiceNamespace namespace : namespaces) {
            if (mapNameSequencePairs == emptyList()) {
                mapNameSequencePairs = new ArrayList(namespaces.size());
            }

            ObjectNamespace mapNamespace = (ObjectNamespace) namespace;
            String mapName = mapNamespace.getObjectName();

            mapNameSequencePairs.add(mapName);
            mapNameSequencePairs.add(metaData.currentSequence(mapName, partitionId));
        }
    }

    private MetaDataGenerator getPartitionMetaDataGenerator(MapService mapService) {
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        MapNearCacheManager mapNearCacheManager = mapServiceContext.getMapNearCacheManager();
        Invalidator invalidator = mapNearCacheManager.getInvalidator();
        return invalidator.getMetaDataGenerator();
    }

    void applyState() {
        MapService mapService = mapReplicationOperation.getService();
        MetaDataGenerator metaDataGenerator = getPartitionMetaDataGenerator(mapService);

        int partitionId = mapReplicationOperation.getPartitionId();

        if (partitionUuid != null) {
            metaDataGenerator.setUuid(partitionId, partitionUuid);
        }

        for (int i = 0; i < mapNameSequencePairs.size(); ) {
            String mapName = (String) mapNameSequencePairs.get(i++);
            long sequence = (Long) mapNameSequencePairs.get(i++);

            metaDataGenerator.setCurrentSequence(mapName, partitionId, sequence);
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

        out.writeInt(mapNameSequencePairs.size());
        for (Object item : mapNameSequencePairs) {
            out.writeObject(item);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        boolean nullUuid = in.readBoolean();
        partitionUuid = nullUuid ? null : new UUID(in.readLong(), in.readLong());

        int size = in.readInt();
        mapNameSequencePairs = new ArrayList(size);
        for (int i = 0; i < size; i++) {
            mapNameSequencePairs.add(in.readObject());
        }
    }

    @Override
    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return MAP_NEAR_CACHE_STATE_HOLDER;
    }
}
