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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;

/**
 * @ali 4/10/13
 */
public class TxnRemoveAllOperation extends CollectionKeyBasedOperation {

    Collection<Long> recordIds;
    transient long begin = -1;
    transient Collection<CollectionRecord> removed;

    public TxnRemoveAllOperation() {
    }

    public TxnRemoveAllOperation(CollectionProxyId proxyId, Data dataKey, Collection<CollectionRecord> records) {
        super(proxyId, dataKey);
        this.recordIds = new ArrayList<Long>();
        for (CollectionRecord record: records){
            recordIds.add(record.getRecordId());
        }
    }

    public void run() throws Exception {
        begin = Clock.currentTimeMillis();
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
        removed = new LinkedList<CollectionRecord>();
        for (Long recordId: recordIds){
            Iterator<CollectionRecord> iter = coll.iterator();
            while (iter.hasNext()){
                CollectionRecord record = iter.next();
                if (record.getRecordId() == recordId){
                    iter.remove();
                    removed.add(record);
                    break;
                }
            }
        }
        if (coll.isEmpty()) {
            removeCollection();
        }

    }

    public void afterRun() throws Exception {
        long elapsed = Math.max(0, Clock.currentTimeMillis()-begin);
        final CollectionService service = getService();
        service.getLocalMultiMapStatsImpl(proxyId).incrementRemoves(elapsed);
        if (removed != null) {
            getOrCreateContainer().update();
            for (CollectionRecord record : removed) {
                publishEvent(EntryEventType.REMOVED, dataKey, record.getObject());
            }
        }
    }

    public Collection<Long> getRecordIds() {
        return recordIds;
    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(recordIds.size());
        for (Long recordId: recordIds){
            out.writeLong(recordId);
        }
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int size = in.readInt();
        recordIds = new ArrayList<Long>();
        for (int i=0; i<size; i++){
            recordIds.add(in.readLong());
        }
    }

    public int getId() {
        return CollectionDataSerializerHook.TXN_REMOVE_ALL;
    }
}
