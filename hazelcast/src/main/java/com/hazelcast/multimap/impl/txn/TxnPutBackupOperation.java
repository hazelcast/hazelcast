/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.multimap.impl.MultiMapValue;
import com.hazelcast.multimap.impl.operations.MultiMapKeyBasedOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.MutatingOperation;

import java.io.IOException;
import java.util.Collection;

public class TxnPutBackupOperation extends MultiMapKeyBasedOperation implements MutatingOperation {

    long recordId;
    Data value;

    public TxnPutBackupOperation() {
    }

    public TxnPutBackupOperation(String name, Data dataKey, long recordId, Data value) {
        super(name, dataKey);
        this.recordId = recordId;
        this.value = value;
    }

    @Override
    public void run() throws Exception {
        MultiMapContainer container = getOrCreateContainer();
        MultiMapValue multiMapValue = container.getOrCreateMultiMapValue(dataKey);
        response = true;
        if (multiMapValue.containsRecordId(recordId)) {
            response = false;
            return;
        }
        Collection<MultiMapRecord> coll = multiMapValue.getCollection(false);
        MultiMapRecord record = new MultiMapRecord(recordId, isBinary() ? value : toObject(value));
        coll.add(record);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(recordId);
        out.writeData(value);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        recordId = in.readLong();
        value = in.readData();
    }

    @Override
    public int getId() {
        return MultiMapDataSerializerHook.TXN_PUT_BACKUP;
    }
}
