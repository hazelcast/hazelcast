/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.operation;

import com.hazelcast.config.IndexConfig;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.record.Records;
import com.hazelcast.map.impl.recordstore.RecordStoreAdapter;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.query.impl.Index;
import com.hazelcast.query.impl.IndexUtils;
import com.hazelcast.query.impl.Indexes;
import com.hazelcast.query.impl.InternalIndex;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.spi.impl.operationservice.BackupAwareOperation;
import com.hazelcast.spi.impl.operationservice.MutatingOperation;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.PartitionAwareOperation;

import java.io.IOException;

public class AddIndexOperation extends MapOperation
        implements PartitionAwareOperation, MutatingOperation, BackupAwareOperation {
    /**
     * Configuration of the index.
     */
    private IndexConfig config;

    public AddIndexOperation() {
        // No-op.
    }

    public AddIndexOperation(String name, IndexConfig config) {
        super(name);

        this.config = IndexUtils.validateAndNormalize(name, config);
    }

    @Override
    public boolean shouldBackup() {
        return mapContainer.getTotalBackupCount() > 0;
    }

    @Override
    public int getSyncBackupCount() {
        return mapContainer.getTotalBackupCount();
    }

    @Override
    public int getAsyncBackupCount() {
        return 0;
    }

    @Override
    public Operation getBackupOperation() {
        return new AddIndexBackupOperation(name, config);
    }

    @Override
    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    @Override
    public void runInternal() {
        int partitionId = getPartitionId();

        Indexes indexes = mapContainer.getIndexes(partitionId);
        RecordStoreAdapter recordStoreAdapter = new RecordStoreAdapter(recordStore);
        InternalIndex index = indexes.addOrGetIndex(config, indexes.isGlobal() ? null : recordStoreAdapter);
        if (index.hasPartitionIndexed(partitionId)) {
            return;
        }

        SerializationService serializationService = getNodeEngine().getSerializationService();

        recordStore.forEach((dataKey, record) -> {
            Object value = Records.getValueOrCachedValue(record, serializationService);
            QueryableEntry queryEntry = mapContainer.newQueryEntry(dataKey, value);
            queryEntry.setRecord(record);
            queryEntry.setStoreAdapter(recordStoreAdapter);
            index.putEntry(queryEntry, null, Index.OperationSource.USER);
        }, false);

        index.markPartitionAsIndexed(partitionId);
    }

    @Override
    public Object getResponse() {
        return Boolean.TRUE;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(config);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        config = in.readObject();
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.ADD_INDEX;
    }

}
