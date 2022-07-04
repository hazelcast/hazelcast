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

package com.hazelcast.replicatedmap.impl.operation;

import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.cluster.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.replicatedmap.impl.ReplicatedMapEventPublishingService;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecordStore;
import com.hazelcast.spi.impl.operationservice.PartitionAwareOperation;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Replicates the update happened on the partition owner to the other nodes.
 */
public class ReplicateUpdateOperation extends AbstractNamedSerializableOperation implements PartitionAwareOperation {

    private VersionResponsePair response;
    private boolean isRemove;
    private String name;
    private Data dataKey;
    private Data dataValue;
    private long ttl;
    private Address origin;

    public ReplicateUpdateOperation() {
    }

    public ReplicateUpdateOperation(String name, Data dataKey, Data dataValue, long ttl, VersionResponsePair response,
                                    boolean isRemove, Address origin) {
        this.name = name;
        this.dataKey = dataKey;
        this.dataValue = dataValue;
        this.ttl = ttl;
        this.response = response;
        this.isRemove = isRemove;
        this.origin = origin;
    }

    @Override
    public void run() throws Exception {
        ReplicatedMapService service = getService();
        ReplicatedRecordStore store = service.getReplicatedRecordStore(name, true, getPartitionId());
        long currentVersion = store.getVersion();
        long updateVersion = response.getVersion();
        if (currentVersion >= updateVersion) {
            ILogger logger = getLogger();
            if (logger.isFineEnabled()) {
                logger.fine("Rejecting stale update received for replicated map '" + name + "' (partitionId " + getPartitionId()
                        + ") (current version " + currentVersion + ") (update version " + updateVersion + ")");
            }
            return;
        }
        Object key = store.marshall(dataKey);
        Object value = store.marshall(dataValue);
        if (isRemove) {
            store.removeWithVersion(key, updateVersion);
        } else {
            store.putWithVersion(key, value, ttl, TimeUnit.MILLISECONDS, false, updateVersion);
        }
        publishEvent();
    }

    private void publishEvent() {
        ReplicatedMapService service = getService();
        ReplicatedMapEventPublishingService eventPublishingService = service.getEventPublishingService();
        Data dataOldValue = getNodeEngine().toData(response.getResponse());
        if (isRemove) {
            eventPublishingService.fireEntryListenerEvent(dataKey, dataOldValue, null, name, origin);
        } else {
            eventPublishingService.fireEntryListenerEvent(dataKey, dataOldValue, dataValue, name, origin);
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        response.writeData(out);
        out.writeString(name);
        IOUtil.writeData(out, dataKey);
        IOUtil.writeData(out, dataValue);
        out.writeLong(ttl);
        out.writeBoolean(isRemove);
        out.writeObject(origin);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        response = new VersionResponsePair();
        response.readData(in);
        name = in.readString();
        dataKey = IOUtil.readData(in);
        dataValue = IOUtil.readData(in);
        ttl = in.readLong();
        isRemove = in.readBoolean();
        origin = in.readObject();
    }

    @Override
    public int getClassId() {
        return ReplicatedMapDataSerializerHook.REPLICATE_UPDATE;
    }

    @Override
    public String getName() {
        return name;
    }
}
