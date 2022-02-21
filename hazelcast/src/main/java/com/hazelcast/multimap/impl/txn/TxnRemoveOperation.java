/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.util.Timer;
import com.hazelcast.multimap.impl.MultiMapContainer;
import com.hazelcast.multimap.impl.MultiMapDataSerializerHook;
import com.hazelcast.multimap.impl.MultiMapRecord;
import com.hazelcast.multimap.impl.MultiMapService;
import com.hazelcast.multimap.impl.MultiMapValue;
import com.hazelcast.multimap.impl.operations.AbstractKeyBasedMultiMapOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.operationservice.BackupAwareOperation;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.MutatingOperation;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

public class TxnRemoveOperation extends AbstractKeyBasedMultiMapOperation implements BackupAwareOperation, MutatingOperation {

    private long recordId;
    private Data value;

    private transient long startTimeNanos;

    public TxnRemoveOperation() {
    }

    public TxnRemoveOperation(String name, Data dataKey, long recordId, Data value) {
        super(name, dataKey);
        this.recordId = recordId;
        this.value = value;
    }

    @Override
    public void run() throws Exception {
        startTimeNanos = Timer.nanos();
        MultiMapContainer container = getOrCreateContainer();
        MultiMapValue multiMapValue = container.getMultiMapValueOrNull(dataKey);
        if (multiMapValue == null || !multiMapValue.containsRecordId(recordId)) {
            response = false;
            return;
        }
        response = true;
        container.update();
        Collection<MultiMapRecord> coll = multiMapValue.getCollection(false);
        Iterator<MultiMapRecord> iter = coll.iterator();
        while (iter.hasNext()) {
            if (iter.next().getRecordId() == recordId) {
                iter.remove();
                break;
            }
        }
        if (coll.isEmpty()) {
            container.delete(dataKey);
        }
    }

    @Override
    public void afterRun() throws Exception {
        long elapsed = Math.max(0, Timer.nanosElapsed(startTimeNanos));
        MultiMapService service = getService();
        service.getLocalMultiMapStatsImpl(name).incrementRemoveLatencyNanos(elapsed);
        if (Boolean.TRUE.equals(response)) {
            publishEvent(EntryEventType.REMOVED, dataKey, null, value);
        }
    }

    @Override
    public boolean shouldBackup() {
        return Boolean.TRUE.equals(response);
    }

    @Override
    public Operation getBackupOperation() {
        return new TxnRemoveBackupOperation(name, dataKey, recordId, value);
    }

    public long getRecordId() {
        return recordId;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(recordId);
        IOUtil.writeData(out, value);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        recordId = in.readLong();
        value = IOUtil.readData(in);
    }

    @Override
    public int getClassId() {
        return MultiMapDataSerializerHook.TXN_REMOVE;
    }
}
