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

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.replicatedmap.impl.ReplicatedMapEventPublishingService;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecordStore;
import com.hazelcast.spi.impl.operationservice.PartitionAwareOperation;
import com.hazelcast.spi.impl.operationservice.MutatingOperation;

import java.io.IOException;

/**
 * Removes the key from replicated map.
 */
public class RemoveOperation extends AbstractReplicatedMapOperation implements PartitionAwareOperation, MutatingOperation {

    private transient ReplicatedMapService service;
    private transient Data oldValue;

    public RemoveOperation() {
    }

    public RemoveOperation(String name, Data key) {
        this.name = name;
        this.key = key;
    }

    @Override
    public void run() throws Exception {
        service = getService();
        ReplicatedRecordStore store = service.getReplicatedRecordStore(name, true, getPartitionId());
        Object removed = store.remove(key);
        oldValue = getNodeEngine().toData(removed);
        response = new VersionResponsePair(removed, store.getVersion());
        Address thisAddress = getNodeEngine().getThisAddress();
        if (!getCallerAddress().equals(thisAddress)) {
            sendUpdateCallerOperation(true);
        }
    }

    @Override
    public void afterRun() throws Exception {
        sendReplicationOperation(true);
        ReplicatedMapEventPublishingService eventPublishingService = service.getEventPublishingService();
        eventPublishingService.fireEntryListenerEvent(key, oldValue, null, name, getCallerAddress());
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeString(name);
        IOUtil.writeData(out, key);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        name = in.readString();
        key = IOUtil.readData(in);
    }

    @Override
    public int getClassId() {
        return ReplicatedMapDataSerializerHook.REMOVE;
    }
}
