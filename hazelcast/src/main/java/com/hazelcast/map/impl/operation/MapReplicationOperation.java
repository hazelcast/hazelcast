/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.Member;
import com.hazelcast.internal.partition.impl.InternalPartitionReplicationEvent;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.record.RecordInfo;
import com.hazelcast.map.impl.record.RecordReplicationInfo;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.impl.Versioned;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationAccessor;
import com.hazelcast.spi.ServiceNamespace;
import com.hazelcast.spi.impl.MutatingOperation;
import com.hazelcast.version.MemberVersion;

import java.io.IOException;
import java.util.Collection;

import static com.hazelcast.map.impl.record.Records.buildRecordInfo;

/**
 * Replicates all IMap-states of this partition to a replica partition.
 */
public class MapReplicationOperation extends Operation implements MutatingOperation, IdentifiedDataSerializable, Versioned {

    private static final MemberVersion V3_9_3 = MemberVersion.of(3, 9, 3);
    @SuppressWarnings("checkstyle:magicnumber")
    private static final int BITMASK_TARGET_SUPPORTS_MAP_INDEX_INFO = 1 << 15;

    // keep these fields `protected`, extended in another context.
    protected final MapReplicationStateHolder mapReplicationStateHolder = new MapReplicationStateHolder(this);
    protected final WriteBehindStateHolder writeBehindStateHolder = new WriteBehindStateHolder(this);
    protected final MapNearCacheStateHolder mapNearCacheStateHolder = new MapNearCacheStateHolder(this);

    public MapReplicationOperation() {
    }

    public MapReplicationOperation(NodeEngine nodeEngine, PartitionContainer container,
                                   InternalPartitionReplicationEvent event) {
        int replicaIndex = event.getReplicaIndex();
        setPartitionId(event.getPartitionId()).setReplicaIndex(replicaIndex);
        Collection<ServiceNamespace> namespaces = container.getAllNamespaces(replicaIndex);
        this.mapReplicationStateHolder.prepare(container, namespaces, replicaIndex);
        this.writeBehindStateHolder.prepare(container, namespaces, replicaIndex);
        this.mapNearCacheStateHolder.prepare(container, namespaces, replicaIndex);
        if (nodeEngine != null) {
            this.setNodeEngine(nodeEngine);
            setupFlagForMapIndexInfos(event.getDestination());
        }
    }

    public MapReplicationOperation(NodeEngine nodeEngine, PartitionContainer container, Collection<ServiceNamespace> namespaces,
                                   InternalPartitionReplicationEvent event) {
        int replicaIndex = event.getReplicaIndex();
        setPartitionId(event.getPartitionId()).setReplicaIndex(replicaIndex);
        this.mapReplicationStateHolder.prepare(container, namespaces, replicaIndex);
        this.writeBehindStateHolder.prepare(container, namespaces, replicaIndex);
        this.mapNearCacheStateHolder.prepare(container, namespaces, replicaIndex);
        if (nodeEngine != null) {
            this.setNodeEngine(nodeEngine);
            setupFlagForMapIndexInfos(event.getDestination());
        }
    }

    @Override
    public void run() {
        mapReplicationStateHolder.applyState();
        writeBehindStateHolder.applyState();
        if (getReplicaIndex() == 0) {
            mapNearCacheStateHolder.applyState();
        }
    }

    @Override
    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    @Override
    protected void writeInternal(final ObjectDataOutput out) throws IOException {
        mapReplicationStateHolder.writeData(out);
        writeBehindStateHolder.writeData(out);
        mapNearCacheStateHolder.writeData(out);
    }

    @Override
    protected void readInternal(final ObjectDataInput in) throws IOException {
        if (OperationAccessor.isFlagSet(this, BITMASK_TARGET_SUPPORTS_MAP_INDEX_INFO)) {
            mapReplicationStateHolder.setIncludeMapIndexInfos(true);
        }
        mapReplicationStateHolder.readData(in);
        writeBehindStateHolder.readData(in);
        mapNearCacheStateHolder.readData(in);
    }

    RecordReplicationInfo createRecordReplicationInfo(Data key, Record record, MapServiceContext mapServiceContext) {
        RecordInfo info = buildRecordInfo(record);
        return new RecordReplicationInfo(key, mapServiceContext.toData(record.getValue()), info);
    }

    RecordStore getRecordStore(String mapName) {
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
    public int getId() {
        return MapDataSerializerHook.MAP_REPLICATION;
    }

    private void setupFlagForMapIndexInfos(Address destination) {
        if (targetSupportsMapIndexInfo(destination)) {
            OperationAccessor.setFlag(this, true, BITMASK_TARGET_SUPPORTS_MAP_INDEX_INFO);
            mapReplicationStateHolder.setIncludeMapIndexInfos(true);
        }
    }

    private boolean targetSupportsMapIndexInfo(Address destination) {
        Member member = getNodeEngine().getClusterService().getMember(destination);
        MemberVersion targetVersion = member.getVersion();
        return targetVersion.compareTo(V3_9_3) > 0;
    }
}
