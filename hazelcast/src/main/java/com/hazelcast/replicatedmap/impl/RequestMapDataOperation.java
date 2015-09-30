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

package com.hazelcast.replicatedmap.impl;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.replicatedmap.impl.operation.SyncReplicatedMapDataOperation;
import com.hazelcast.replicatedmap.impl.record.RecordMigrationInfo;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecord;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecordStore;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.OperationService;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Collects and sends the replicated map data from the executing node to the caller via
 * {@link SyncReplicatedMapDataOperation}.
 */
public class RequestMapDataOperation extends AbstractOperation {

    private static ILogger logger = Logger.getLogger(RequestMapDataOperation.class.getName());

    String name;

    public RequestMapDataOperation() {
    }

    public RequestMapDataOperation(String name) {
        this.name = name;
    }

    @Override
    public void run() throws Exception {
        logger.finest("Caller { " + getCallerAddress() + " } requested copy of map -> " + name
                + ", on partition -> " + getPartitionId());
        ReplicatedMapService service = getService();
        PartitionContainer container = service.getPartitionContainer(getPartitionId());
        ReplicatedRecordStore store = container.getRecordStore(name);
        if (store == null) {
            logger.finest("No data is found on this store to respond data request");
            return;
        }
        long version = store.getVersion();
        Set<RecordMigrationInfo> recordSet = getRecordSet(store);
        if (recordSet.isEmpty()) {
            logger.finest("No data is found on this store to respond data request");
            return;
        }
        SyncReplicatedMapDataOperation op = new SyncReplicatedMapDataOperation(name, recordSet, version);
        op.setPartitionId(getPartitionId());
        op.setValidateTarget(false);
        OperationService operationService = getNodeEngine().getOperationService();
        operationService.invokeOnTarget(ReplicatedMapService.SERVICE_NAME, op, getCallerAddress());
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }


    private Set<RecordMigrationInfo> getRecordSet(ReplicatedRecordStore store) {
        Set<RecordMigrationInfo> recordSet = new HashSet<RecordMigrationInfo>(store.size());
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
}
