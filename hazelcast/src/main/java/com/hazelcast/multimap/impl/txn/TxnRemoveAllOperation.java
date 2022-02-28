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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;

public class TxnRemoveAllOperation extends AbstractKeyBasedMultiMapOperation implements BackupAwareOperation, MutatingOperation {

    private Collection<Long> recordIds;

    private transient long startTimeNanos;
    private transient Collection<MultiMapRecord> removed;

    public TxnRemoveAllOperation() {
    }

    public TxnRemoveAllOperation(String name, Data dataKey, Collection<MultiMapRecord> records) {
        super(name, dataKey);
        this.recordIds = new ArrayList<Long>();
        for (MultiMapRecord record : records) {
            recordIds.add(record.getRecordId());
        }
    }

    @Override
    public void run() throws Exception {
        startTimeNanos = Timer.nanos();
        MultiMapContainer container = getOrCreateContainer();
        MultiMapValue multiMapValue = container.getOrCreateMultiMapValue(dataKey);
        for (Long recordId : recordIds) {
            if (!multiMapValue.containsRecordId(recordId)) {
                response = false;
                return;
            }
        }
        response = true;
        container.update();
        Collection<MultiMapRecord> coll = multiMapValue.getCollection(false);
        removed = new LinkedList<MultiMapRecord>();
        for (Long recordId : recordIds) {
            Iterator<MultiMapRecord> iter = coll.iterator();
            while (iter.hasNext()) {
                MultiMapRecord record = iter.next();
                if (record.getRecordId() == recordId) {
                    iter.remove();
                    removed.add(record);
                    break;
                }
            }
        }
        if (coll.isEmpty()) {
            container.delete(dataKey);
        }
    }

    @Override
    public void afterRun() throws Exception {
        long elapsedNanos = Math.max(0, Timer.nanosElapsed(startTimeNanos));
        MultiMapService service = getService();
        service.getLocalMultiMapStatsImpl(name).incrementRemoveLatencyNanos(elapsedNanos);
        if (removed != null) {
            for (MultiMapRecord record : removed) {
                publishEvent(EntryEventType.REMOVED, dataKey, null, record.getObject());
            }
        }
    }

    @Override
    public boolean shouldBackup() {
        return Boolean.TRUE.equals(response);
    }

    @Override
    public Operation getBackupOperation() {
        return new TxnRemoveAllBackupOperation(name, dataKey, recordIds);
    }

    public Collection<Long> getRecordIds() {
        return recordIds;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(recordIds.size());
        for (Long recordId : recordIds) {
            out.writeLong(recordId);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int size = in.readInt();
        recordIds = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            recordIds.add(in.readLong());
        }
    }

    @Override
    public int getClassId() {
        return MultiMapDataSerializerHook.TXN_REMOVE_ALL;
    }
}
