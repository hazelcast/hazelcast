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

import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecordStore;
import com.hazelcast.spi.PartitionAwareOperation;
import java.io.IOException;

import static com.hazelcast.replicatedmap.impl.record.AbstractReplicatedRecordStore.TOMBSTONE_REMOVAL_PERIOD_MS;

/**
 * Removes the key from replicated map.
 */
public class RemoveOperation extends AbstractReplicatedMapOperation implements PartitionAwareOperation {

    private transient ReplicatedMapService service;
    private transient ReplicatedRecordStore store;


    public RemoveOperation() {
    }

    public RemoveOperation(String name, Data key) {
        this.name = name;
        this.key = key;
    }

    @Override
    public void beforeRun() throws Exception {
        this.ttl = TOMBSTONE_REMOVAL_PERIOD_MS;
    }

    @Override
    public void run() throws Exception {
        service = getService();
        store = service.getReplicatedRecordStore(name, true, getPartitionId());
        response = new VersionResponsePair(store.remove(key), store.getVersion());
        Address thisAddress = getNodeEngine().getThisAddress();
        if (!getCallerAddress().equals(thisAddress)) {
            sendUpdateCallerOperation(true);
        }
    }

    @Override
    public void afterRun() throws Exception {
        sendReplicationOperation(true);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeData(key);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        key = in.readData();
    }
}
