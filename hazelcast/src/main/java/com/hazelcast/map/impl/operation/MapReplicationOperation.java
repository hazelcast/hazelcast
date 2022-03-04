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

package com.hazelcast.map.impl.operation;

import com.hazelcast.internal.services.ServiceNamespace;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.io.IOException;
import java.util.Collection;

/**
 * Replicates all IMap-states of this partition to a replica partition.
 */
public class MapReplicationOperation extends Operation
        implements IdentifiedDataSerializable {

    protected MapReplicationStateHolder mapReplicationStateHolder;
    protected WriteBehindStateHolder writeBehindStateHolder;
    protected MapNearCacheStateHolder mapNearCacheStateHolder;

    private transient NativeOutOfMemoryError oome;

    public MapReplicationOperation() {
    }

    public MapReplicationOperation(PartitionContainer container,
                                   Collection<ServiceNamespace> namespaces, int partitionId, int replicaIndex) {

        setPartitionId(partitionId)
                .setReplicaIndex(replicaIndex);

        this.mapReplicationStateHolder = new MapReplicationStateHolder();
        this.mapReplicationStateHolder.setOperation(this);
        this.mapReplicationStateHolder.prepare(container, namespaces, replicaIndex);

        this.writeBehindStateHolder = new WriteBehindStateHolder();
        this.writeBehindStateHolder.setMapReplicationOperation(this);
        this.writeBehindStateHolder.prepare(container, namespaces, replicaIndex);

        this.mapNearCacheStateHolder = new MapNearCacheStateHolder();
        this.mapNearCacheStateHolder.setMapReplicationOperation(this);
        this.mapNearCacheStateHolder.prepare(container, namespaces);
    }

    @Override
    public void run() {
        try {
            mapReplicationStateHolder.applyState();
            writeBehindStateHolder.applyState();
            if (getReplicaIndex() == 0) {
                mapNearCacheStateHolder.applyState();
            }
        } catch (Throwable e) {
            getLogger().severe("map replication operation failed for partitionId="
                    + getPartitionId(), e);

            disposePartition();

            if (e instanceof NativeOutOfMemoryError) {
                oome = (NativeOutOfMemoryError) e;
            }
        }
    }

    @Override
    public void afterRun() throws Exception {
        try {
            disposePartition();

            if (oome != null) {
                getLogger().warning(oome.getMessage());
            }
        } finally {
            super.afterRun();
        }
    }

    private void disposePartition() {
        for (String mapName : mapReplicationStateHolder.data.keySet()) {
            dispose(mapName);
        }
    }

    @Override
    public void onExecutionFailure(Throwable e) {
        disposePartition();
        super.onExecutionFailure(e);
    }

    private void dispose(String mapName) {
        int partitionId = getPartitionId();
        MapService mapService = getService();
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        RecordStore recordStore = mapServiceContext.getExistingRecordStore(partitionId, mapName);
        if (recordStore != null) {
            recordStore.disposeDeferredBlocks();
        }
    }

    @Override
    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeObject(mapReplicationStateHolder);
        out.writeObject(writeBehindStateHolder);
        out.writeObject(mapNearCacheStateHolder);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        mapReplicationStateHolder = in.readObject();
        mapReplicationStateHolder.setOperation(this);
        writeBehindStateHolder = in.readObject();
        writeBehindStateHolder.setMapReplicationOperation(this);
        mapNearCacheStateHolder = in.readObject();
        mapNearCacheStateHolder.setMapReplicationOperation(this);
    }

    public RecordStore getRecordStore(String mapName) {
        final boolean skipLoadingOnRecordStoreCreate = true;
        MapService mapService = getService();
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        return mapServiceContext.getRecordStore(getPartitionId(), mapName, skipLoadingOnRecordStoreCreate);
    }

    @Override
    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.MAP_REPLICATION;
    }

    @Override
    public boolean requiresTenantContext() {
        return true;
    }
}
