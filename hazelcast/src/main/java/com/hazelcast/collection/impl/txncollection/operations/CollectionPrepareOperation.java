/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.collection.impl.collection.CollectionDataSerializerHook;
import com.hazelcast.collection.impl.collection.operations.CollectionBackupAwareOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.Operation;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;

public class CollectionPrepareOperation extends CollectionBackupAwareOperation {

    private String transactionId;
    private long[] itemIds;

    public CollectionPrepareOperation() {
    }

    @SuppressFBWarnings("EI_EXPOSE_REP")
    public CollectionPrepareOperation(int partitionId, String name, String serviceName, long[] itemIds, String transactionId) {
        super(name);
        setPartitionId(partitionId);
        setServiceName(serviceName);
        this.itemIds = itemIds;
        this.transactionId = transactionId;
    }

    @Override
    public boolean shouldBackup() {
        return true;
    }

    @Override
    public Operation getBackupOperation() {
        return new CollectionPrepareBackupOperation(name, itemIds, transactionId);
    }

    @Override
    public void run() throws Exception {
        CollectionContainer collectionContainer = getOrCreateContainer();
        for (long itemId : itemIds) {
            collectionContainer.ensureReserve(Math.abs(itemId));
        }
    }

    @Override
    public int getId() {
        return CollectionDataSerializerHook.COLLECTION_PREPARE;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(transactionId);
        out.writeLongArray(itemIds);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        transactionId = in.readUTF();
        itemIds = in.readLongArray();
    }
}
