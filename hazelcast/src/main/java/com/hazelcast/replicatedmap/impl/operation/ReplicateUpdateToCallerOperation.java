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

package com.hazelcast.replicatedmap.impl.operation;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecordStore;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.impl.operationservice.impl.InvocationRegistry;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * This operation will update the local record store with the update received from local/remote partition owner.
 */
public class ReplicateUpdateToCallerOperation extends AbstractOperation implements PartitionAwareOperation {

    private static ILogger logger = Logger.getLogger(ReplicateUpdateToCallerOperation.class.getName());
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
        ReplicatedMapService service = getService();
        ReplicatedRecordStore store = service.getReplicatedRecordStore(name, true, getPartitionId());
        long currentVersion = store.getVersion();
        long updateVersion = response.getVersion();
        if (currentVersion >= updateVersion) {
            logger.finest("Stale update received for replicated map -> " + name + ",  partitionId -> "
                    + getPartitionId() + " , current version -> " + currentVersion + ", update version -> "
                    + updateVersion + ", rejecting update!");
            return;
        }
        Object key = store.marshall(dataKey);
        Object value = store.marshall(dataValue);
        if (isRemove) {
            store.remove(key);
        } else {
            store.put(key, value, ttl, TimeUnit.MILLISECONDS, true);
        }
        store.setVersion(updateVersion);
    }


    @Override
    public void afterRun() throws Exception {
        OperationServiceImpl operationService = (OperationServiceImpl) getNodeEngine().getOperationService();
        InvocationRegistry registry = operationService.getInvocationsRegistry();
        registry.notifyBackupComplete(callId);
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeLong(callId);
        out.writeData(dataKey);
        out.writeData(dataValue);
        response.writeData(out);
        out.writeLong(ttl);
        out.writeBoolean(isRemove);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        callId = in.readLong();
        dataKey = in.readData();
        dataValue = in.readData();
        response = new VersionResponsePair();
        response.readData(in);
        ttl = in.readLong();
        isRemove = in.readBoolean();
    }
}
