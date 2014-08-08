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

package com.hazelcast.multimap.impl.txn;

import com.hazelcast.multimap.impl.MultiMapContainer;
import com.hazelcast.multimap.impl.MultiMapDataSerializerHook;
import com.hazelcast.multimap.impl.MultiMapRecord;
import com.hazelcast.multimap.impl.MultiMapWrapper;
import com.hazelcast.multimap.impl.operations.MultiMapKeyBasedOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

public class TxnRemoveAllBackupOperation extends MultiMapKeyBasedOperation {

    Collection<Long> recordIds;

    public TxnRemoveAllBackupOperation() {
    }

    public TxnRemoveAllBackupOperation(String name, Data dataKey, Collection<Long> recordIds) {
        super(name, dataKey);
        this.recordIds = recordIds;
    }

    public void run() throws Exception {
        MultiMapContainer container = getOrCreateContainer();
        MultiMapWrapper wrapper = container.getOrCreateMultiMapWrapper(dataKey);
        response = true;
        for (Long recordId : recordIds) {
            if (!wrapper.containsRecordId(recordId)) {
                response = false;
                return;
            }
        }
        Collection<MultiMapRecord> coll = wrapper.getCollection(false);
        for (Long recordId : recordIds) {
            Iterator<MultiMapRecord> iter = coll.iterator();
            while (iter.hasNext()) {
                MultiMapRecord record = iter.next();
                if (record.getRecordId() == recordId) {
                    iter.remove();
                    break;
                }
            }
        }
        if (coll.isEmpty()) {
            delete();
        }
    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(recordIds.size());
        for (Long recordId : recordIds) {
            out.writeLong(recordId);
        }
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int size = in.readInt();
        recordIds = new ArrayList<Long>();
        for (int i = 0; i < size; i++) {
            recordIds.add(in.readLong());
        }
    }

    public int getId() {
        return MultiMapDataSerializerHook.TXN_REMOVE_ALL_BACKUP;
    }
}
