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

package com.hazelcast.collection.impl.txncollection.operations;

import com.hazelcast.collection.impl.collection.CollectionContainer;
import com.hazelcast.collection.impl.collection.CollectionDataSerializerHook;
import com.hazelcast.collection.impl.collection.operations.CollectionOperation;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.util.UUIDSerializationUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.operationservice.MutatingOperation;

import java.io.IOException;
import java.util.UUID;

import static com.hazelcast.collection.impl.collection.CollectionContainer.INVALID_ITEM_ID;

public class CollectionReserveRemoveOperation extends CollectionOperation implements MutatingOperation {

    private UUID transactionId;
    private Data value;
    private long reservedItemId = INVALID_ITEM_ID;

    public CollectionReserveRemoveOperation() {
    }

    public CollectionReserveRemoveOperation(String name, long reservedItemId, Data value, UUID transactionId) {
        super(name);
        this.reservedItemId = reservedItemId;
        this.value = value;
        this.transactionId = transactionId;
    }

    @Override
    public int getClassId() {
        return CollectionDataSerializerHook.COLLECTION_RESERVE_REMOVE;
    }

    @Override
    public void run() throws Exception {
        CollectionContainer collectionContainer = getOrCreateContainer();
        response = collectionContainer.reserveRemove(reservedItemId, value, transactionId);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(reservedItemId);
        IOUtil.writeData(out, value);
        UUIDSerializationUtil.writeUUID(out, transactionId);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        reservedItemId = in.readLong();
        value = IOUtil.readData(in);
        transactionId = UUIDSerializationUtil.readUUID(in);
    }
}
