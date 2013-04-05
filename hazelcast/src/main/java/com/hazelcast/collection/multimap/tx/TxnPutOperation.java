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
import com.hazelcast.collection.operations.CollectionBackupAwareOperation;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Notifier;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.WaitNotifyKey;
import com.hazelcast.util.Clock;

import java.io.IOException;
import java.util.Collection;

/**
 * @ali 4/2/13
 */
public class TxnPutOperation extends CollectionBackupAwareOperation implements Notifier{

    long recordId;
    Data value;
    transient long begin = -1;
    transient boolean notify = true;

    public TxnPutOperation() {
    }

    public TxnPutOperation(CollectionProxyId proxyId, Data dataKey, Data value, long recordId, int threadId) {
        super(proxyId, dataKey, threadId);
        this.recordId = recordId;
        this.value = value;
    }

    public void run() throws Exception {
        begin = Clock.currentTimeMillis();
        CollectionContainer container = getOrCreateContainer();
        CollectionWrapper wrapper = container.getOrCreateCollectionWrapper(dataKey);
        if (wrapper.containsRecordId(recordId)){
            response = false;
            notify = false;
            return;
        }
        Collection<CollectionRecord> coll = wrapper.getCollection();
        CollectionRecord record = new CollectionRecord(recordId, isBinary() ? value : toObject(value));
        response = coll.add(record);
        container.unlock(dataKey, getCallerUuid(), threadId);
    }

    public void afterRun() throws Exception {
        long elapsed = Math.max(0, Clock.currentTimeMillis()-begin);
        getOrCreateContainer().getOperationsCounter().incrementPuts(elapsed);
        if (Boolean.TRUE.equals(response)) {
            publishEvent(EntryEventType.ADDED, dataKey, value);
        }
    }

    public boolean shouldBackup() {
        return notify;
    }

    public Operation getBackupOperation() {
        return new TxnPutBackupOperation(proxyId, dataKey, value, recordId, threadId, getCallerUuid());
    }

    public boolean shouldNotify() {
        return notify;
    }

    public WaitNotifyKey getNotifiedKey() {
        return getWaitKey();
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

}
