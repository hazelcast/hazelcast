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
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * This operation will update the local record store with the update received from local/remote partition owner.
 */
public class ReplicateUpdateToCallerOperation extends AbstractSerializableOperation implements PartitionAwareOperation {

    private String name;
    private long callId;
    private Data dataKey;
    private Data dataValue;
    private VersionResponsePair response;
    private long ttl;
    private boolean isRemove;

    public ReplicateUpdateToCallerOperation() {
    }

    public ReplicateUpdateToCallerOperation(String name, long callId, Data dataKey, Data dataValue, VersionResponsePair response,
                                            long ttl, boolean isRemove) {
        this.name = name;
        this.callId = callId;
        this.dataKey = dataKey;
        this.dataValue = dataValue;
        this.response = response;
        this.ttl = ttl;
        this.isRemove = isRemove;
    }

    @Override
    public void run() throws Exception {
        ILogger logger = getLogger();
        ReplicatedMapService service = getService();
        ReplicatedRecordStore store = service.getReplicatedRecordStore(name, true, getPartitionId());
        long currentVersion = store.getVersion();
        long updateVersion = response.getVersion();
        if (currentVersion >= updateVersion) {
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
            store.putWithVersion(key, value, ttl, TimeUnit.MILLISECONDS, true, updateVersion);
        }

        publishEvent();
    }

    @Override
    public void afterRun() throws Exception {
        notifyCaller();
    }

    private void publishEvent() {
        ReplicatedMapService service = getService();
        ReplicatedMapEventPublishingService eventPublishingService = service.getEventPublishingService();
        Address thisAddress = getNodeEngine().getThisAddress();
        Data dataOldValue = getNodeEngine().toData(response.getResponse());
        if (isRemove) {
            eventPublishingService.fireEntryListenerEvent(dataKey, dataOldValue, null, name, thisAddress);
        } else {
            eventPublishingService.fireEntryListenerEvent(dataKey, dataOldValue, dataValue, name, thisAddress);
        }
    }

    private void notifyCaller() {
        OperationServiceImpl operationService = (OperationServiceImpl) getNodeEngine().getOperationService();
        operationService.getBackupHandler().notifyBackupComplete(callId);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeString(name);
        out.writeLong(callId);
        IOUtil.writeData(out, dataKey);
        IOUtil.writeData(out, dataValue);
        response.writeData(out);
        out.writeLong(ttl);
        out.writeBoolean(isRemove);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        name = in.readString();
        callId = in.readLong();
        dataKey = IOUtil.readData(in);
        dataValue = IOUtil.readData(in);
        response = new VersionResponsePair();
        response.readData(in);
        ttl = in.readLong();
        isRemove = in.readBoolean();
    }

    @Override
    public int getClassId() {
        return ReplicatedMapDataSerializerHook.REPLICATE_UPDATE_TO_CALLER;
    }
}
