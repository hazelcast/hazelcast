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

package com.hazelcast.ringbuffer.impl.operations;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.ringbuffer.impl.RingbufferContainer;
import com.hazelcast.ringbuffer.impl.RingbufferService;
import com.hazelcast.spi.AbstractOperation;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.ringbuffer.impl.RingbufferDataSerializerHook.F_ID;
import static com.hazelcast.ringbuffer.impl.RingbufferDataSerializerHook.REPLICATION_OPERATION;
import static com.hazelcast.ringbuffer.impl.RingbufferService.SERVICE_NAME;

public class ReplicationOperation extends AbstractOperation
        implements IdentifiedDataSerializable {

    private Map<String, RingbufferContainer> migrationData;

    public ReplicationOperation() {
    }

    public ReplicationOperation(Map<String, RingbufferContainer> migrationData,
                                int partitionId, int replicaIndex) {
        setPartitionId(partitionId).setReplicaIndex(replicaIndex);
        this.migrationData = migrationData;
    }

    @Override
    public void run() {
        RingbufferService service = getService();
        for (Map.Entry<String, RingbufferContainer> entry : migrationData.entrySet()) {
            String name = entry.getKey();
            RingbufferContainer ringbuffer = entry.getValue();
            service.addRingbuffer(name, ringbuffer);
        }
    }

    @Override
    public String getServiceName() {
        return SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public int getId() {
        return REPLICATION_OPERATION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeInt(migrationData.size());
        for (Map.Entry<String, RingbufferContainer> entry : migrationData.entrySet()) {
            String ringbufferName = entry.getKey();
            out.writeUTF(ringbufferName);
            RingbufferContainer container = entry.getValue();
            container.writeData(out);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        int mapSize = in.readInt();
        migrationData = new HashMap<String, RingbufferContainer>(mapSize);
        for (int i = 0; i < mapSize; i++) {
            String name = in.readUTF();
            RingbufferContainer container = new RingbufferContainer(name);
            container.readData(in);
            migrationData.put(name, container);
        }
    }
}
