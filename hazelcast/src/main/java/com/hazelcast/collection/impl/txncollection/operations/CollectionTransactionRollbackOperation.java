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
import com.hazelcast.collection.impl.collection.operations.CollectionOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

public class CollectionTransactionRollbackOperation extends CollectionOperation {

    private String transactionId;

    public CollectionTransactionRollbackOperation() {
    }

    public CollectionTransactionRollbackOperation(String name, String transactionId) {
        super(name);
        this.transactionId = transactionId;
    }

    @Override
    public void run() throws Exception {
        CollectionContainer collectionContainer = getOrCreateContainer();
        collectionContainer.rollbackTransaction(transactionId);
    }

    @Override
    public int getId() {
        return CollectionDataSerializerHook.TX_ROLLBACK;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(transactionId);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        transactionId = in.readUTF();
    }
}
