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

import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.replicatedmap.impl.PartitionContainer;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecordStore;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.spi.impl.operationservice.PartitionAwareOperation;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.replicatedmap.impl.ReplicatedMapService.INVOCATION_TRY_COUNT;
import static com.hazelcast.replicatedmap.impl.ReplicatedMapService.SERVICE_NAME;
import static com.hazelcast.internal.util.MapUtil.createConcurrentHashMap;

/**
 * Checks whether replica version is in sync with the primary.
 * If not, it will request the correct state via {@link RequestMapDataOperation}
 */
public class CheckReplicaVersionOperation extends AbstractSerializableOperation implements PartitionAwareOperation {

    private Map<String, Long> versions;

    public CheckReplicaVersionOperation() {
    }

    public CheckReplicaVersionOperation(PartitionContainer container) {
        ConcurrentMap<String, ReplicatedRecordStore> stores = container.getStores();
        versions = createConcurrentHashMap(stores.size());
        for (Map.Entry<String, ReplicatedRecordStore> storeEntry : stores.entrySet()) {
            String name = storeEntry.getKey();
            ReplicatedRecordStore store = storeEntry.getValue();
            long version = store.getVersion();
            versions.put(name, version);
        }
    }

    @Override
    public void run() throws Exception {
        ILogger logger = getLogger();
        int partitionId = getPartitionId();
        ReplicatedMapService service = getService();
        PartitionContainer container = service.getPartitionContainer(getPartitionId());
        ConcurrentMap<String, ReplicatedRecordStore> stores = container.getStores();
        for (Map.Entry<String, Long> entry : versions.entrySet()) {
            String name = entry.getKey();
            Long version = entry.getValue();
            ReplicatedRecordStore store = stores.get(name);
            if (store == null) {
                if (logger.isFineEnabled()) {
                    logger.fine("Missing store on the replica of replicated map '" + name
                            + "' (partitionId " + partitionId + ") (owner version " + version + ")");
                }
                requestDataFromOwner(name);
            } else if (store.isStale(version)) {
                if (logger.isFineEnabled()) {
                    logger.fine("Stale replica on replicated map '" + name + "' (partitionId " + partitionId
                            + ") (owner version " + version + ") (replica version " + store.getVersion() + ")");
                }
                requestDataFromOwner(name);
            }
        }
    }

    private void requestDataFromOwner(String name) {
        OperationService operationService = getNodeEngine().getOperationService();
        Operation op = new RequestMapDataOperation(name);
        operationService
                .createInvocationBuilder(SERVICE_NAME, op, getPartitionId())
                .setTryCount(INVOCATION_TRY_COUNT)
                .invoke();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeInt(versions.size());
        for (Map.Entry<String, Long> entry : versions.entrySet()) {
            out.writeString(entry.getKey());
            out.writeLong(entry.getValue());
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        versions = new ConcurrentHashMap<>();
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            String name = in.readString();
            Long version = in.readLong();
            versions.put(name, version);
        }
    }

    @Override
    public int getClassId() {
        return ReplicatedMapDataSerializerHook.CHECK_REPLICA_VERSION;
    }
}
