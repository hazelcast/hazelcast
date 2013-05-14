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

package com.hazelcast.collection.multimap.tx;

import com.hazelcast.collection.*;
import com.hazelcast.collection.operations.CollectionKeyBasedOperation;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.util.Clock;

import java.io.IOException;
import java.util.Collection;

/**
 * @ali 4/2/13
 */
public class TxnPutOperation extends CollectionKeyBasedOperation {

    long recordId;
    Data value;
    transient long begin = -1;

    public TxnPutOperation() {
    }

    public TxnPutOperation(CollectionProxyId proxyId, Data dataKey, Data value, long recordId) {
        super(proxyId, dataKey);
        this.recordId = recordId;
        this.value = value;
    }

    public void run() throws Exception {
        begin = Clock.currentTimeMillis();
        CollectionContainer container = getOrCreateContainer();
        CollectionWrapper wrapper = container.getOrCreateCollectionWrapper(dataKey);
        response = true;
        if (wrapper.containsRecordId(recordId)){
            response = false;
            return;
        }
        Collection<CollectionRecord> coll = wrapper.getCollection();
        CollectionRecord record = new CollectionRecord(recordId, isBinary() ? value : toObject(value));
        coll.add(record);
    }

    public void afterRun() throws Exception {
        long elapsed = Math.max(0, Clock.currentTimeMillis()-begin);
        final CollectionService service = getService();
        service.getLocalMultiMapStatsImpl(proxyId).incrementPuts(elapsed);
        if (Boolean.TRUE.equals(response)) {
            publishEvent(EntryEventType.ADDED, dataKey, value);
        }
    }

    public long getRecordId() {
        return recordId;
    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(recordId);
        value.writeData(out);
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        recordId = in.readLong();
        value = new Data();
        value.readData(in);
    }

    public int getId() {
        return CollectionDataSerializerHook.TXN_PUT;
    }

}
