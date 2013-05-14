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

package com.hazelcast.collection.operations;

import com.hazelcast.collection.CollectionContainer;
import com.hazelcast.collection.CollectionDataSerializerHook;
import com.hazelcast.collection.CollectionProxyId;
import com.hazelcast.collection.CollectionRecord;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;
import com.hazelcast.util.Clock;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 * @ali 1/16/13
 */
public class PutOperation extends CollectionBackupAwareOperation {

    Data value;

    int index = -1;

    transient long begin = -1;
    transient long recordId;

    public PutOperation() {
    }

    public PutOperation(CollectionProxyId proxyId, Data dataKey, int threadId, Data value, int index) {
        super(proxyId, dataKey, threadId);
        this.value = value;
        this.index = index;
    }

    public void run() throws Exception {
        begin = Clock.currentTimeMillis();
        CollectionContainer container = getOrCreateContainer();
        recordId = container.nextId();
        CollectionRecord record = new CollectionRecord(recordId, isBinary() ? value : toObject(value));
        Collection<CollectionRecord> coll = container.getOrCreateCollectionWrapper(dataKey).getCollection();
        if (index == -1) {
            response = coll.add(record);
        } else {
            try {
                ((List<CollectionRecord>) coll).add(index, record);
                response = true;
            } catch (IndexOutOfBoundsException e) {
                response = e;
            }
        }
    }

    public void afterRun() throws Exception {
        long elapsed = Math.max(0, Clock.currentTimeMillis() - begin);
        if (Boolean.TRUE.equals(response)) {
            publishEvent(EntryEventType.ADDED, dataKey, value);
        }
    }

    public Operation getBackupOperation() {
        return new PutBackupOperation(proxyId, dataKey, value, recordId, index);
    }

    public boolean shouldBackup() {
        return Boolean.TRUE.equals(response);
    }

    public void onWaitExpire() {
        getResponseHandler().sendResponse(false);
    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(index);
        value.writeData(out);
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        index = in.readInt();
        value = IOUtil.readData(in);
    }

    public int getId() {
        return CollectionDataSerializerHook.PUT;
    }
}
