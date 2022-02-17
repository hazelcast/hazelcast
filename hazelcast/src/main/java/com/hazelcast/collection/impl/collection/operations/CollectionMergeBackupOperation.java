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

package com.hazelcast.collection.impl.collection.operations;

import com.hazelcast.collection.impl.collection.CollectionContainer;
import com.hazelcast.collection.impl.collection.CollectionDataSerializerHook;
import com.hazelcast.collection.impl.collection.CollectionItem;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.operationservice.BackupOperation;
import com.hazelcast.internal.services.RemoteService;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

/**
 * Creates backups for merged collection items after split-brain healing with a {@link SplitBrainMergePolicy}.
 *
 * @since 3.10
 */
public class CollectionMergeBackupOperation extends CollectionOperation implements BackupOperation {

    private Collection<CollectionItem> backupItems;

    public CollectionMergeBackupOperation() {
    }

    public CollectionMergeBackupOperation(String name, Collection<CollectionItem> backupItems) {
        super(name);
        this.backupItems = backupItems;
    }

    @Override
    public void run() throws Exception {
        CollectionContainer container = getOrCreateContainer();
        if (backupItems.isEmpty()) {
            RemoteService service = getService();
            service.destroyDistributedObject(name);
        } else {
            Map<Long, CollectionItem> backupMap = container.getMap();
            backupMap.clear();
            for (CollectionItem backupItem : backupItems) {
                backupMap.put(backupItem.getItemId(), backupItem);
            }
        }
    }

    @Override
    public int getClassId() {
        return CollectionDataSerializerHook.COLLECTION_MERGE_BACKUP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(backupItems.size());
        for (CollectionItem backupItem : backupItems) {
            out.writeObject(backupItem);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int size = in.readInt();
        backupItems = new ArrayList<CollectionItem>(size);
        for (int i = 0; i < size; i++) {
            CollectionItem backupItem = in.readObject();
            backupItems.add(backupItem);
        }
    }
}
