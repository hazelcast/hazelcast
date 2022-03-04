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

package com.hazelcast.multimap.impl.operations;

import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.multimap.impl.MultiMapContainer;
import com.hazelcast.multimap.impl.MultiMapDataSerializerHook;
import com.hazelcast.multimap.impl.MultiMapRecord;
import com.hazelcast.multimap.impl.MultiMapValue;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.operationservice.BackupOperation;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import static com.hazelcast.internal.util.MapUtil.createHashMap;

/**
 * Creates backups for merged {@link MultiMapRecord} after split-brain healing with a {@link SplitBrainMergePolicy}.
 *
 * @since 3.10
 */
public class MergeBackupOperation extends AbstractMultiMapOperation implements BackupOperation {

    private Map<Data, Collection<MultiMapRecord>> backupEntries;

    public MergeBackupOperation() {
    }

    MergeBackupOperation(String name, Map<Data, Collection<MultiMapRecord>> backupEntries) {
        super(name);
        this.backupEntries = backupEntries;
    }

    @Override
    public void run() throws Exception {
        response = true;
        MultiMapContainer container = getOrCreateContainerWithoutAccess();
        for (Map.Entry<Data, Collection<MultiMapRecord>> entry : backupEntries.entrySet()) {
            Data key = entry.getKey();
            Collection<MultiMapRecord> value = entry.getValue();
            if (value.isEmpty()) {
                container.remove(key, false);
            } else {
                MultiMapValue containerValue = container.getOrCreateMultiMapValue(key);
                Collection<MultiMapRecord> collection = containerValue.getCollection(false);
                collection.clear();
                if (!collection.addAll(value)) {
                    response = false;
                }
            }
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(backupEntries.size());
        for (Map.Entry<Data, Collection<MultiMapRecord>> entry : backupEntries.entrySet()) {
            IOUtil.writeData(out, entry.getKey());
            Collection<MultiMapRecord> collection = entry.getValue();
            out.writeInt(collection.size());
            for (MultiMapRecord record : collection) {
                out.writeObject(record);
            }
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int size = in.readInt();
        backupEntries = createHashMap(size);
        for (int i = 0; i < size; i++) {
            Data key = IOUtil.readData(in);
            int collectionSize = in.readInt();
            Collection<MultiMapRecord> collection = new ArrayList<MultiMapRecord>(collectionSize);
            for (int j = 0; j < collectionSize; j++) {
                MultiMapRecord record = in.readObject();
                collection.add(record);
            }
            backupEntries.put(key, collection);
        }
    }

    @Override
    public int getClassId() {
        return MultiMapDataSerializerHook.MERGE_BACKUP_OPERATION;
    }
}
