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
import com.hazelcast.collection.impl.collection.CollectionDataSerializerHook;
import com.hazelcast.collection.impl.collection.operations.CollectionOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import java.io.IOException;

public class CollectionReserveAddOperation extends CollectionOperation {

    private String transactionId;
    private Data value;

    public CollectionReserveAddOperation() {
    }

    public CollectionReserveAddOperation(String name, String transactionId, Data value) {
        super(name);
        this.transactionId = transactionId;
        this.value = value;
    }

    @Override
    public void run() throws Exception {
        CollectionContainer collectionContainer = getOrCreateContainer();
        response = collectionContainer.reserveAdd(transactionId, value);
    }

    @Override
    public int getId() {
        return CollectionDataSerializerHook.COLLECTION_RESERVE_ADD;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(transactionId);
        out.writeData(value);

    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        transactionId = in.readUTF();
        value = in.readData();
    }
}
