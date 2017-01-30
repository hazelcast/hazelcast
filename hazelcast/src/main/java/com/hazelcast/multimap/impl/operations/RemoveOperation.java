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

package com.hazelcast.multimap.impl.operations;

import com.hazelcast.core.EntryEventType;
import com.hazelcast.multimap.impl.MultiMapDataSerializerHook;
import com.hazelcast.multimap.impl.MultiMapRecord;
import com.hazelcast.multimap.impl.MultiMapValue;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

public class RemoveOperation extends MultiMapBackupAwareOperation {

    private Data value;
    private long recordId;

    public RemoveOperation() {
    }

    public RemoveOperation(String name, Data dataKey, long threadId, Data value) {
        super(name, dataKey, threadId);
        this.value = value;
    }

    @Override
    public void run() throws Exception {
        response = false;
        MultiMapValue multiMapValue = getMultiMapValueOrNull();
        if (multiMapValue == null) {
            return;
        }
        Collection<MultiMapRecord> coll = multiMapValue.getCollection(false);
        MultiMapRecord record = new MultiMapRecord(isBinary() ? value : toObject(value));
        Iterator<MultiMapRecord> iter = coll.iterator();
        while (iter.hasNext()) {
            MultiMapRecord r = iter.next();
            if (r.equals(record)) {
                iter.remove();
                recordId = r.getRecordId();
                response = true;
                if (coll.isEmpty()) {
                    delete();
                }
                break;
            }
        }
    }

    @Override
    public void afterRun() throws Exception {
        if (Boolean.TRUE.equals(response)) {
            getOrCreateContainer().update();
            publishEvent(EntryEventType.REMOVED, dataKey, null, value);
        }
    }

    @Override
    public boolean shouldBackup() {
        return Boolean.TRUE.equals(response);
    }

    @Override
    public Operation getBackupOperation() {
        return new RemoveBackupOperation(name, dataKey, recordId);
    }

    @Override
    public void onWaitExpire() {
        sendResponse(false);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeData(value);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        value = in.readData();
    }

    @Override
    public int getId() {
        return MultiMapDataSerializerHook.REMOVE;
    }
}
