/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.queue;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.BackupOperation;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * @author ali 12/19/12
 */
public class DrainBackupOperation extends QueueOperation implements BackupOperation {

    //can be null
    Set<Long> itemIdSet;

    public DrainBackupOperation() {
    }

    public DrainBackupOperation(String name) {
        super(name);
    }

    public DrainBackupOperation(String name, Set<Long> itemIdSet) {
        super(name);
        this.itemIdSet = itemIdSet;
    }

    public void run() throws Exception {
        getOrCreateContainer().drainFromBackup(itemIdSet);
    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        if (itemIdSet == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeInt(itemIdSet.size());
            for (Long itemId : itemIdSet) {
                out.writeLong(itemId);
            }
        }
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        if (in.readBoolean()) {
            int size = in.readInt();
            itemIdSet = new HashSet<Long>(size);
            for (int i = 0; i < size; i++) {
                itemIdSet.add(in.readLong());
            }
        }
    }

    public int getId() {
        return QueueDataSerializerHook.DRAIN_BACKUP;
    }
}
