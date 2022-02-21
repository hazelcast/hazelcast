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
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.operationservice.BackupOperation;

import java.io.IOException;
import java.util.Set;

import static com.hazelcast.internal.util.SetUtil.createHashSet;

/**
 * This class triggers backup method for items' ID.
 */
public class CompareAndRemoveBackupOperation extends QueueOperation implements BackupOperation {

    private Set<Long> keySet;

    public CompareAndRemoveBackupOperation() {
    }

    public CompareAndRemoveBackupOperation(String name, Set<Long> keySet) {
        super(name);
        this.keySet = keySet;
    }

    @Override
    public void run() throws Exception {
        QueueContainer queueContainer = getContainer();
        queueContainer.compareAndRemoveBackup(keySet);
        response = true;
    }

    @Override
    public int getClassId() {
        return QueueDataSerializerHook.COMPARE_AND_REMOVE_BACKUP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(keySet.size());
        for (Long key : keySet) {
            out.writeLong(key);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int size = in.readInt();
        keySet = createHashSet(size);
        for (int i = 0; i < size; i++) {
            keySet.add(in.readLong());
        }
    }
}
