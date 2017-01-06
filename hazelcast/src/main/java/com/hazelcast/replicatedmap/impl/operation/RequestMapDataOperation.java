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
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.replicatedmap.impl.PartitionContainer;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.replicatedmap.impl.record.RecordMigrationInfo;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecord;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecordStore;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.SetUtil;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

import static com.hazelcast.replicatedmap.impl.ReplicatedMapService.INVOCATION_TRY_COUNT;
import static com.hazelcast.replicatedmap.impl.ReplicatedMapService.SERVICE_NAME;

/**
 * Collects and sends the replicated map data from the executing node to the caller via
 * {@link SyncReplicatedMapDataOperation}.
 */
public class RequestMapDataOperation extends AbstractSerializableOperation {

    String name;

    public RequestMapDataOperation() {
    }

    public RequestMapDataOperation(String name) {
        this.name = name;
    }

    @Override
    public void run() throws Exception {
        ILogger logger = getLogger();
        int partitionId = getPartitionId();
        if (logger.isFineEnabled()) {
            logger.fine("Caller { " + getCallerAddress() + " } requested copy of map: " + name
                    + " partitionId=" + partitionId);
        }
        ReplicatedMapService service = getService();
        PartitionContainer container = service.getPartitionContainer(partitionId);
        ReplicatedRecordStore store = container.getRecordStore(name);
        if (store == null) {
            if (logger.isFineEnabled()) {
                logger.fine("No store is found for map: " + name + " to respond data request. partitionId=" + partitionId);
            }

            return;
        }
        long version = store.getVersion();
        Set<RecordMigrationInfo> recordSet = getRecordSet(store);
        if (recordSet.isEmpty()) {
            if (logger.isFineEnabled()) {
                logger.fine("No data is found on this store for map: " + name +  " to respond data request. partitionId="
                        + partitionId);
            }
            return;
        }
        SyncReplicatedMapDataOperation op = new SyncReplicatedMapDataOperation(name, recordSet, version);
        op.setPartitionId(partitionId);
        op.setValidateTarget(false);
        OperationService operationService = getNodeEngine().getOperationService();
        operationService
                .createInvocationBuilder(SERVICE_NAME, op, getCallerAddress())
                .setTryCount(INVOCATION_TRY_COUNT)
                .invoke();
    }


    private Set<RecordMigrationInfo> getRecordSet(ReplicatedRecordStore store) {
        Set<RecordMigrationInfo> recordSet = SetUtil.createHashSet(store.size());
        Iterator<ReplicatedRecord> iterator = store.recordIterator();
        while (iterator.hasNext()) {
            ReplicatedRecord record = iterator.next();
            SerializationService serializationService = getNodeEngine().getSerializationService();
            Data dataKey = serializationService.toData(record.getKeyInternal());
            Data dataValue = serializationService.toData(record.getValueInternal());
            recordSet.add(new RecordMigrationInfo(dataKey, dataValue, record.getTtlMillis()));
        }
        return recordSet;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        name = in.readUTF();
    }

    @Override
    public int getId() {
        return ReplicatedMapDataSerializerHook.REQUEST_MAP_DATA;
    }
}
