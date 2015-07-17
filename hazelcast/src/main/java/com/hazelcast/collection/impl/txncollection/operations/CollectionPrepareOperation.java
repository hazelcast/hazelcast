/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.collection.impl.txncollection.operations;

import com.hazelcast.collection.impl.collection.CollectionContainer;
import com.hazelcast.collection.impl.collection.operations.CollectionBackupAwareOperation;
import com.hazelcast.collection.impl.collection.CollectionDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.Operation;
import java.io.IOException;

public class CollectionPrepareOperation extends CollectionBackupAwareOperation {

    private boolean removeOperation;
    private String transactionId;
    private long itemId = -1;

    public CollectionPrepareOperation() {
    }

    public CollectionPrepareOperation(int partitionId, String name, String serviceName, long itemId, String transactionId,
                                      boolean removeOperation) {
        super(name);
        setPartitionId(partitionId);
        setServiceName(serviceName);
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
    public void run() throws Exception {
        CollectionContainer collectionContainer = getOrCreateContainer();
        collectionContainer.ensureReserve(itemId);
    }

    @Override
    public int getId() {
        return CollectionDataSerializerHook.COLLECTION_PREPARE;
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
