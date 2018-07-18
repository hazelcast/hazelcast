/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.multimap.impl.MultiMapContainer;
import com.hazelcast.multimap.impl.MultiMapDataSerializerHook;
import com.hazelcast.multimap.impl.MultiMapRecord;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.MutatingOperation;
import java.io.IOException;

public class RemoveOperation extends AbstractBackupAwareMultiMapOperation implements MutatingOperation {

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
        MultiMapContainer container = getOrCreateContainer();
        MultiMapRecord record = new MultiMapRecord(isBinary() ? value : toObject(value));
        long recordId = container.removeValue(dataKey, record);
        if (recordId >= 0) {
            this.recordId = recordId;
            response = true;
        } else {
            response = false;
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
