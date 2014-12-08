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

package com.hazelcast.multimap.impl.operations;

import com.hazelcast.core.EntryEventType;
import com.hazelcast.multimap.impl.MultiMapContainer;
import com.hazelcast.multimap.impl.MultiMapDataSerializerHook;
import com.hazelcast.multimap.impl.MultiMapRecord;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

public class PutOperation extends MultiMapBackupAwareOperation {

    Data value;
    int index = -1;
    long recordId;

    public PutOperation() {
    }

    public PutOperation(String name, Data dataKey, long threadId, Data value, int index) {
        super(name, dataKey, threadId);
        this.value = value;
        this.index = index;
    }

    public void run() throws Exception {
        MultiMapContainer container = getOrCreateContainer();
        recordId = container.nextId();
        MultiMapRecord record = new MultiMapRecord(recordId, isBinary() ? value : toObject(value));
        Collection<MultiMapRecord> coll = container.getOrCreateMultiMapWrapper(dataKey).getCollection(false);
        if (index == -1) {
            response = coll.add(record);
        } else {
            try {
                ((List<MultiMapRecord>) coll).add(index, record);
                response = true;
            } catch (IndexOutOfBoundsException e) {
                response = e;
            }
        }
    }

    public void afterRun() throws Exception {
        if (Boolean.TRUE.equals(response)) {
            publishEvent(EntryEventType.ADDED, dataKey, value);
        }
    }

    public Operation getBackupOperation() {
        return new PutBackupOperation(name, dataKey, value, recordId, index);
    }

    public boolean shouldBackup() {
        return Boolean.TRUE.equals(response);
    }

    public void onWaitExpire() {
        getResponseHandler().sendResponse(false);
    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(index);
        out.writeData(value);
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        index = in.readInt();
        value = in.readData();
    }

    public int getId() {
        return MultiMapDataSerializerHook.PUT;
    }
}
