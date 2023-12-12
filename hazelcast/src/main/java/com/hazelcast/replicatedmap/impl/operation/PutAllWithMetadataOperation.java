/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.partition.IPartitionService;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.replicatedmap.impl.record.RecordMigrationInfo;
import com.hazelcast.replicatedmap.impl.record.ReplicatedMapEntryView;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecordStore;
import com.hazelcast.spi.impl.operationservice.MutatingOperation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Puts a list of {@link com.hazelcast.replicatedmap.impl.record.ReplicatedMapEntryView}s to the replicated map.
 */
public class PutAllWithMetadataOperation extends AbstractNamedSerializableOperation implements MutatingOperation {

    private String name;
    private List<ReplicatedMapEntryView<Data, Data>> entryViews;

    public PutAllWithMetadataOperation() {
    }

    public PutAllWithMetadataOperation(String name, List<ReplicatedMapEntryView<Data, Data>> entryViews) {
        this.name = name;
        this.entryViews = entryViews;
    }

    @Override
    public void run() throws Exception {
        ReplicatedMapService service = getService();
        ReplicatedRecordStore store = service.getReplicatedRecordStore(name, true, getPartitionId());
        int partitionId = getPartitionId();
        IPartitionService partitionService = getNodeEngine().getPartitionService();
        Collection<RecordMigrationInfo> records = new ArrayList<>(entryViews.size());
        for (ReplicatedMapEntryView<Data, Data> entryView : entryViews) {
            Data key = entryView.getKey();
            if (partitionId != partitionService.getPartitionId(key)) {
                continue;
            }
            RecordMigrationInfo record = new RecordMigrationInfo(entryView.getKey(), entryView.getValue(), entryView.getTtl());
            record.setCreationTime(entryView.getCreationTime());
            record.setHits(entryView.getHits());
            record.setLastAccessTime(entryView.getLastAccessTime());
            record.setLastUpdateTime(entryView.getLastUpdateTime());
            records.add(record);
        }
        store.putRecords(records, store.getVersion() + 1);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeString(name);
        out.writeObject(entryViews);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        name = in.readString();
        entryViews = in.readObject();
    }

    @Override
    public int getClassId() {
        return ReplicatedMapDataSerializerHook.PUT_ALL_WITH_METADATA;
    }

    @Override
    public String getName() {
        return name;
    }
}
