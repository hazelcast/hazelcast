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
import java.util.Iterator;

/**
 * @ali 4/5/13
 */
public class TxnRemoveOperation extends CollectionBackupAwareOperation implements Notifier {

    long recordId;
    Data value;
    transient long begin = -1;
    transient boolean notify = false;

    public TxnRemoveOperation() {
    }

    public TxnRemoveOperation(CollectionProxyId proxyId, Data dataKey, int threadId, long recordId, Data value) {
        super(proxyId, dataKey, threadId);
        this.recordId = recordId;
        this.value = value;
    }

    public void run() throws Exception {
        begin = Clock.currentTimeMillis();
        CollectionContainer container = getOrCreateContainer();
        CollectionWrapper wrapper = container.getCollectionWrapper(dataKey);
        response = false;
        if (wrapper == null || !wrapper.containsRecordId(recordId)) {
            return;
        }
        Collection<CollectionRecord> coll = wrapper.getCollection();
        Iterator<CollectionRecord> iter = coll.iterator();
        while (iter.hasNext()){
            if (iter.next().getRecordId() == recordId){
                iter.remove();
                response = true;
                notify = true;
                if (coll.isEmpty()) {
                    removeCollection();
                }
                container.unlock(dataKey, getCallerUuid(), threadId);
                break;
            }
        }
    }

    public void afterRun() throws Exception {
        long elapsed = Math.max(0, Clock.currentTimeMillis()-begin);
        getOrCreateContainer().getOperationsCounter().incrementRemoves(elapsed);
        if (Boolean.TRUE.equals(response)) {
            getOrCreateContainer().update();
            publishEvent(EntryEventType.REMOVED, dataKey, value);
        }
    }

    public Operation getBackupOperation() {
        return new TxnRemoveBackupOperation(proxyId,dataKey,recordId,value,threadId,getCallerUuid());
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
