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

package com.hazelcast.multimap.txn;

import com.hazelcast.multimap.MultiMapContainer;
import com.hazelcast.multimap.MultiMapDataSerializerHook;
import com.hazelcast.multimap.MultiMapRecord;
import com.hazelcast.multimap.MultiMapWrapper;
import com.hazelcast.multimap.operations.MultiMapKeyBasedOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;

import java.io.IOException;
import java.util.Collection;

/**
 * @ali 10/18/13
 */
public class TxnPutBackupOperation extends MultiMapKeyBasedOperation {

    long recordId;
    Data value;

    public TxnPutBackupOperation() {
    }

    public TxnPutBackupOperation(String name, Data dataKey, long recordId, Data value) {
        super(name, dataKey);
        this.recordId = recordId;
        this.value = value;
    }

    public void run() throws Exception {
        MultiMapContainer container = getOrCreateContainer();
        MultiMapWrapper wrapper = container.getOrCreateMultiMapWrapper(dataKey);
        response = true;
        if (wrapper.containsRecordId(recordId)){
            response = false;
            return;
        }
        Collection<MultiMapRecord> coll = wrapper.getCollection();
        MultiMapRecord record = new MultiMapRecord(recordId, isBinary() ? value : toObject(value));
        coll.add(record);
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
        return MultiMapDataSerializerHook.TXN_PUT_BACKUP;
    }
}
