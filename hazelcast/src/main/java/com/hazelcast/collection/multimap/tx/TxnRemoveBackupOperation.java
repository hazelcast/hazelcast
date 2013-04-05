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
import com.hazelcast.spi.BackupOperation;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

/**
 * @ali 4/5/13
 */
public class TxnRemoveBackupOperation extends CollectionKeyBasedOperation implements BackupOperation {

    long recordId;
    Data value;
    int threadId;
    String caller;

    public TxnRemoveBackupOperation() {
    }

    public TxnRemoveBackupOperation(CollectionProxyId proxyId, Data dataKey, long recordId, Data value, int threadId, String caller) {
        super(proxyId, dataKey);
        this.recordId = recordId;
        this.value = value;
        this.threadId = threadId;
        this.caller = caller;
    }

    public void run() throws Exception {
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
                if (coll.isEmpty()) {
                    removeCollection();
                }
                container.unlock(dataKey, caller, threadId);
                break;
            }
        }
    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(recordId);
        out.writeInt(threadId);
        out.writeUTF(caller);
        value.writeData(out);
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        recordId = in.readLong();
        threadId = in.readInt();
        caller = in.readUTF();
        value = new Data();
        value.readData(in);
    }
}
