/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.collection.multimap.tx;

import com.hazelcast.collection.CollectionContainer;
import com.hazelcast.collection.CollectionProxyId;
import com.hazelcast.collection.CollectionRecord;
import com.hazelcast.collection.CollectionWrapper;
import com.hazelcast.collection.operations.CollectionKeyBasedOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;

import java.io.IOException;

/**
 * @ali 3/29/13
 */
public class TxnReservePutOperation extends CollectionKeyBasedOperation {

    Data dataValue;

    public TxnReservePutOperation() {
    }

    public TxnReservePutOperation(CollectionProxyId proxyId, Data dataKey, Data dataValue) {
        super(proxyId, dataKey);
        this.dataValue = dataValue;
    }

    public void run() throws Exception {
        CollectionContainer container = getOrCreateContainer();
        CollectionRecord record = new CollectionRecord(container.nextId(), isBinary() ? dataValue : toObject(dataValue));
        CollectionWrapper wrapper = container.getOrCreateCollectionWrapper(dataKey);
        wrapper.reservePut(record);
    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        dataValue.writeData(out);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        dataValue = new Data();
        dataValue.readData(in);
    }
}
