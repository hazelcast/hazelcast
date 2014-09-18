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

import com.hazelcast.collection.CollectionBackupAwareOperation;
import com.hazelcast.collection.CollectionDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.Operation;
import java.io.IOException;

public class CollectionPrepareOperation extends CollectionBackupAwareOperation {

    boolean removeOperation;
    String transactionId;

    private long itemId = -1;

    public CollectionPrepareOperation() {
    }

    public CollectionPrepareOperation(String name, long itemId, String transactionId, boolean removeOperation) {
        super(name);
        this.itemId = itemId;
        this.removeOperation = removeOperation;
        this.transactionId = transactionId;
    }

    @Override
    public boolean shouldBackup() {
        return true;
    }

    @Override
    public Operation getBackupOperation() {
        return new CollectionPrepareBackupOperation(name, itemId, transactionId, removeOperation);
    }

    @Override
    public int getId() {
        return CollectionDataSerializerHook.COLLECTION_PREPARE;
    }

    @Override
    public void beforeRun() throws Exception {
    }

    @Override
    public void run() throws Exception {
        getOrCreateContainer().ensureReserve(itemId);
    }

    @Override
    public void afterRun() throws Exception {
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(itemId);
        out.writeBoolean(removeOperation);
        out.writeUTF(transactionId);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        itemId = in.readLong();
        removeOperation = in.readBoolean();
        transactionId = in.readUTF();
    }
}
