/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.VersionAware;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.impl.Versioned;
import com.hazelcast.ringbuffer.impl.RingbufferContainer;
import com.hazelcast.ringbuffer.impl.RingbufferService;
import com.hazelcast.spi.ObjectNamespace;
import com.hazelcast.spi.Operation;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import static com.hazelcast.internal.cluster.Versions.V3_9;
import static com.hazelcast.ringbuffer.impl.RingbufferDataSerializerHook.F_ID;
import static com.hazelcast.ringbuffer.impl.RingbufferDataSerializerHook.REPLICATION_OPERATION;
import static com.hazelcast.ringbuffer.impl.RingbufferService.SERVICE_NAME;

public class ReplicationOperation extends Operation implements IdentifiedDataSerializable, Versioned {

    private Map<ObjectNamespace, RingbufferContainer> migrationData;

    public ReplicationOperation() {
    }

    public ReplicationOperation(Map<ObjectNamespace, RingbufferContainer> migrationData,
                                int partitionId, int replicaIndex) {
        setPartitionId(partitionId).setReplicaIndex(replicaIndex);
        this.migrationData = migrationData;
    }

    @Override
    public void run() {
        final RingbufferService service = getService();
        for (Map.Entry<ObjectNamespace, RingbufferContainer> entry : migrationData.entrySet()) {
            final ObjectNamespace ns = entry.getKey();
            final RingbufferContainer ringbuffer = entry.getValue();
            service.addRingbuffer(getPartitionId(), ringbuffer, getRingbufferConfig(service, ns));
        }
    }

    private RingbufferConfig getRingbufferConfig(RingbufferService service, ObjectNamespace ns) {
        return service.getRingbufferConfig(ns.getObjectName());
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
        for (Entry<ObjectNamespace, RingbufferContainer> entry : migrationData.entrySet()) {
            final ObjectNamespace ns = entry.getKey();
            if (isGreaterOrEqualV39(out)) {
                out.writeObject(ns);
            } else {
                out.writeUTF(ns.getObjectName());
            }
            RingbufferContainer container = entry.getValue();
            container.writeData(out);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        int mapSize = in.readInt();
        migrationData = new HashMap<ObjectNamespace, RingbufferContainer>(mapSize);
        for (int i = 0; i < mapSize; i++) {
            final ObjectNamespace namespace = isGreaterOrEqualV39(in)
                    ? (ObjectNamespace) in.readObject()
                    : RingbufferService.getRingbufferNamespace(in.readUTF());
            final RingbufferContainer container = new RingbufferContainer(namespace);
            container.readData(in);
            migrationData.put(namespace, container);
        }
    }

    private static boolean isGreaterOrEqualV39(VersionAware versionAware) {
        return versionAware.getVersion().isGreaterOrEqual(V3_9);
    }
}
