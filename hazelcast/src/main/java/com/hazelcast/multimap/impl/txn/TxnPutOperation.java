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

import com.hazelcast.core.EntryEventType;
import com.hazelcast.multimap.impl.MultiMapContainer;
import com.hazelcast.multimap.impl.MultiMapDataSerializerHook;
import com.hazelcast.multimap.impl.MultiMapRecord;
import com.hazelcast.multimap.impl.MultiMapService;
import com.hazelcast.multimap.impl.MultiMapWrapper;
import com.hazelcast.multimap.impl.operations.MultiMapKeyBasedOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.util.Clock;
import java.io.IOException;
import java.util.Collection;

public class TxnPutOperation extends MultiMapKeyBasedOperation implements BackupAwareOperation {

    long recordId;
    Data value;
    long begin = -1;

    public TxnPutOperation() {
    }

    public TxnPutOperation(String name, Data dataKey, Data value, long recordId) {
        super(name, dataKey);
        this.recordId = recordId;
        this.value = value;
    }

    public void run() throws Exception {
        begin = Clock.currentTimeMillis();
        MultiMapContainer container = getOrCreateContainer();
        MultiMapWrapper wrapper = container.getOrCreateMultiMapWrapper(dataKey);
        response = true;
        if (wrapper.containsRecordId(recordId)) {
            response = false;
            return;
        }
        Collection<MultiMapRecord> coll = wrapper.getCollection(false);
        MultiMapRecord record = new MultiMapRecord(recordId, isBinary() ? value : toObject(value));
        coll.add(record);
    }

    public void afterRun() throws Exception {
        long elapsed = Math.max(0, Clock.currentTimeMillis() - begin);
        final MultiMapService service = getService();
        service.getLocalMultiMapStatsImpl(name).incrementPuts(elapsed);
        if (Boolean.TRUE.equals(response)) {
            publishEvent(EntryEventType.ADDED, dataKey, value);
        }
    }

    public long getRecordId() {
        return recordId;
    }

    public boolean shouldBackup() {
        return Boolean.TRUE.equals(response);
    }

    public Operation getBackupOperation() {
        return new TxnPutBackupOperation(name, dataKey, recordId, value);
    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(recordId);
        out.writeData(value);
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        recordId = in.readLong();
        value = in.readData();
    }

    public int getId() {
        return MultiMapDataSerializerHook.TXN_PUT;
    }

}
