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
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupOperation;
import java.io.IOException;

public class CollectionTxnAddBackupOperation extends CollectionOperation implements BackupOperation {

    private long itemId;

    private Data value;

    public CollectionTxnAddBackupOperation() {
    }

    public CollectionTxnAddBackupOperation(String name, long itemId, Data value) {
        super(name);
        this.itemId = itemId;
        this.value = value;
    }

    @Override
    public int getId() {
        return CollectionDataSerializerHook.COLLECTION_TXN_ADD_BACKUP;
    }

    @Override
    public void beforeRun() throws Exception {
    }

    @Override
    public void run() throws Exception {
        getOrCreateContainer().commitAddBackup(itemId, value);
        response = true;
    }

    @Override
    public void afterRun() throws Exception {
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(itemId);
        out.writeData(value);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        itemId = in.readLong();
        value = in.readData();
    }
}
