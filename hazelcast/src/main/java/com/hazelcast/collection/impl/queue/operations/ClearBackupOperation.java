/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.spi.impl.MutatingOperation;

import java.io.IOException;
import java.util.Set;

import static com.hazelcast.util.SetUtil.createHashSet;

/**
 * Store items' ID as set when ClearOperation run.
 */
public class ClearBackupOperation extends QueueOperation implements BackupOperation, MutatingOperation {

    private Set<Long> itemIdSet;

    public ClearBackupOperation() {
    }

    public ClearBackupOperation(String name, Set<Long> itemIdSet) {
        super(name);
        this.itemIdSet = itemIdSet;
    }

    @Override
    public void run() throws Exception {
        QueueContainer queueContainer = getContainer();
        queueContainer.clearBackup(itemIdSet);
        response = true;
    }

    @Override
    public int getId() {
        return QueueDataSerializerHook.CLEAR_BACKUP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(itemIdSet.size());
        for (Long itemId : itemIdSet) {
            out.writeLong(itemId);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int size = in.readInt();
        itemIdSet = createHashSet(size);
        for (int i = 0; i < size; i++) {
            itemIdSet.add(in.readLong());
        }
    }
}
