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

package com.hazelcast.collection.impl.queue.operations;

import com.hazelcast.collection.impl.queue.QueueContainer;
import com.hazelcast.collection.impl.queue.QueueDataSerializerHook;
import com.hazelcast.collection.impl.queue.QueueService;
import com.hazelcast.config.Config;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;

import java.io.IOException;
import java.util.Map;

import static com.hazelcast.util.MapUtil.createHashMap;

/**
 * Replication operation for the Queue.
 */

public class QueueReplicationOperation extends Operation implements IdentifiedDataSerializable {

    private Map<String, QueueContainer> migrationData;

    public QueueReplicationOperation() {
    }

    public QueueReplicationOperation(Map<String, QueueContainer> migrationData, int partitionId, int replicaIndex) {
        setPartitionId(partitionId).setReplicaIndex(replicaIndex);
        this.migrationData = migrationData;
    }

    @Override
    public void run() {
        QueueService service = getService();
        NodeEngine nodeEngine = getNodeEngine();
        Config config = nodeEngine.getConfig();
        for (Map.Entry<String, QueueContainer> entry : migrationData.entrySet()) {
            String name = entry.getKey();
            QueueContainer container = entry.getValue();
            QueueConfig conf = config.findQueueConfig(name);
            container.setConfig(conf, nodeEngine, service);
            service.addContainer(name, container);
        }
    }

    @Override
    public String getServiceName() {
        return QueueService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return QueueDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return QueueDataSerializerHook.QUEUE_REPLICATION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeInt(migrationData.size());
        for (Map.Entry<String, QueueContainer> entry : migrationData.entrySet()) {
            out.writeUTF(entry.getKey());
            QueueContainer container = entry.getValue();
            container.writeData(out);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        int mapSize = in.readInt();
        migrationData = createHashMap(mapSize);
        for (int i = 0; i < mapSize; i++) {
            String name = in.readUTF();
            QueueContainer container = new QueueContainer(name);
            container.readData(in);
            migrationData.put(name, container);
        }
    }
}
