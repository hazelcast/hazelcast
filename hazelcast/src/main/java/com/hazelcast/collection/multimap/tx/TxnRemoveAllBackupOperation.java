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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

/**
 * @ali 4/10/13
 */
public class TxnRemoveAllBackupOperation extends CollectionKeyBasedOperation implements BackupOperation {

    Collection<Long> recordIds;
    String caller;
    int threadId;

    public TxnRemoveAllBackupOperation() {
    }

    public TxnRemoveAllBackupOperation(CollectionProxyId proxyId, Data dataKey, String caller, int threadId, Collection<Long> recordIds) {
        super(proxyId, dataKey);
        this.recordIds = recordIds;
        this.threadId = threadId;
        this.caller = caller;
    }

    public void run() throws Exception {
        CollectionContainer container = getOrCreateContainer();
        CollectionWrapper wrapper = container.getOrCreateCollectionWrapper(dataKey);
        response = true;
        for (Long recordId: recordIds){
            if(!wrapper.containsRecordId(recordId)){
                response = false;
                return;
            }
        }
        Collection<CollectionRecord> coll = wrapper.getCollection();
        for (Long recordId: recordIds){
            Iterator<CollectionRecord> iter = coll.iterator();
            while (iter.hasNext()){
                CollectionRecord record = iter.next();
                if (record.getRecordId() == recordId){
                    iter.remove();
                    break;
                }
            }
        }
        if (coll.isEmpty()) {
            removeCollection();
        }
        container.unlock(dataKey, caller, threadId);
    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(caller);
        out.writeInt(threadId);
        out.writeInt(recordIds.size());
        for (Long recordId: recordIds){
            out.writeLong(recordId);
        }
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        caller = in.readUTF();
        threadId = in.readInt();
        int size = in.readInt();
        recordIds = new ArrayList<Long>();
        for (int i=0; i<size; i++){
            recordIds.add(in.readLong());
        }
    }
}
