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

package com.hazelcast.collection.txn;

import com.hazelcast.collection.CollectionDataSerializerHook;
import com.hazelcast.collection.CollectionOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.BackupOperation;

import java.io.IOException;

/**
 * @ali 9/3/13
 */
public class CollectionRollbackBackupOperation extends CollectionOperation implements BackupOperation {

    private long itemId;

    private boolean removeOperation;

    public CollectionRollbackBackupOperation() {
    }

    public CollectionRollbackBackupOperation(String name, long itemId, boolean removeOperation) {
        super(name);
        this.itemId = itemId;
        this.removeOperation = removeOperation;
    }

    public int getId() {
        return CollectionDataSerializerHook.COLLECTION_ROLLBACK_BACKUP;
    }

    public void beforeRun() throws Exception {

    }

    public void run() throws Exception {
        if (removeOperation){
            getOrCreateContainer().rollbackRemoveBackup(itemId);
        } else {
            getOrCreateContainer().rollbackAddBackup(itemId);
        }
    }

    public void afterRun() throws Exception {

    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(itemId);
        out.writeBoolean(removeOperation);
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        itemId = in.readLong();
        removeOperation = in.readBoolean();
    }
}
