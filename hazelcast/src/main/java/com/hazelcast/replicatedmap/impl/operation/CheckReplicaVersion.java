/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.replicatedmap.impl.PartitionContainer;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecordStore;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.PartitionAwareOperation;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.replicatedmap.impl.ReplicatedMapService.INVOCATION_TRY_COUNT;
import static com.hazelcast.replicatedmap.impl.ReplicatedMapService.SERVICE_NAME;

/**
 * Checks whether replica version is in sync with the primary.
 * If not, it will request the correct state via {@link RequestMapDataOperation}
 */
public class CheckReplicaVersion extends AbstractOperation implements PartitionAwareOperation {

    private static ILogger logger = Logger.getLogger(CheckReplicaVersion.class.getName());

    private Map<String, Long> versions;


    public CheckReplicaVersion() {
    }

    public CheckReplicaVersion(PartitionContainer container) {
        versions = new ConcurrentHashMap<String, Long>();
        ConcurrentMap<String, ReplicatedRecordStore> stores = container.getStores();
        for (Map.Entry<String, ReplicatedRecordStore> storeEntry : stores.entrySet()) {
            String name = storeEntry.getKey();
            ReplicatedRecordStore store = storeEntry.getValue();
            long version = store.getVersion();
            versions.put(name, version);
        }
    }

    @Override
    public void run() throws Exception {
        ReplicatedMapService service = getService();
        PartitionContainer container = service.getPartitionContainer(getPartitionId());
        ConcurrentMap<String, ReplicatedRecordStore> stores = container.getStores();
        for (Map.Entry<String, Long> entry : versions.entrySet()) {
            String name = entry.getKey();
            Long version = entry.getValue();
            ReplicatedRecordStore store = stores.get(name);
            if (store == null) {
                logger.finest("Missing store on the replica ! Owner version -> " + version);
                requestDataFromOwner(name);
            } else if (!version.equals(store.getVersion())) {
                logger.finest("Version mismatch on the replica ! Owner version ->  " + version
                        + ", Replica version -> " + store.getVersion());
                requestDataFromOwner(name);
            }
        }
    }

    private void requestDataFromOwner(String name) {
        OperationService operationService = getNodeEngine().getOperationService();
        RequestMapDataOperation requestMapDataOperation = new RequestMapDataOperation(name);
        operationService
                .createInvocationBuilder(SERVICE_NAME, requestMapDataOperation, getPartitionId())
                .setTryCount(INVOCATION_TRY_COUNT)
                .invoke();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeInt(versions.size());
        for (Map.Entry<String, Long> entry : versions.entrySet()) {
            out.writeUTF(entry.getKey());
            out.writeLong(entry.getValue());
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        versions = new ConcurrentHashMap<String, Long>();
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            String name = in.readUTF();
            Long version = in.readLong();
            versions.put(name, version);
        }
    }
}
