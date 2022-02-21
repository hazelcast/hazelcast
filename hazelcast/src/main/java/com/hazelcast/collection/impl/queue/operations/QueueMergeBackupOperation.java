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

package com.hazelcast.collection.impl.queue.operations;

import com.hazelcast.collection.impl.queue.QueueContainer;
import com.hazelcast.collection.impl.queue.QueueDataSerializerHook;
import com.hazelcast.collection.impl.queue.QueueItem;
import com.hazelcast.collection.impl.queue.QueueService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.operationservice.MutatingOperation;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

/**
 * Creates backups for merged queue items after split-brain healing with a {@link SplitBrainMergePolicy}.
 *
 * @since 3.10
 */
public class QueueMergeBackupOperation extends QueueOperation implements MutatingOperation {

    private Collection<QueueItem> backupItems;

    public QueueMergeBackupOperation() {
    }

    public QueueMergeBackupOperation(String name, Collection<QueueItem> backupItems) {
        super(name);
        this.backupItems = backupItems;
    }

    @Override
    public void run() {
        if (backupItems.isEmpty()) {
            QueueService service = getService();
            service.destroyDistributedObject(name);
            return;
        }
        QueueContainer container = getContainer();
        container.clear();

        Map<Long, QueueItem> backupMap = container.getBackupMap();
        for (QueueItem backupItem : backupItems) {
            backupMap.put(backupItem.getItemId(), backupItem);
        }
    }

    @Override
    public void afterRun() {
        getQueueService().getLocalQueueStatsImpl(name).incrementOtherOperations();
    }

    @Override
    public int getClassId() {
        return QueueDataSerializerHook.MERGE_BACKUP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(backupItems.size());
        for (QueueItem backupItem : backupItems) {
            out.writeObject(backupItem);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int size = in.readInt();
        backupItems = new ArrayList<QueueItem>(size);
        for (int i = 0; i < size; i++) {
            QueueItem backupItem = in.readObject();
            backupItems.add(backupItem);
        }
    }
}
