/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.collection.operations;

import com.hazelcast.collection.CollectionProxyType;
import com.hazelcast.collection.CollectionRecord;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;

import java.io.IOException;
import java.util.Collection;

/**
 * @ali 1/16/13
 */
public class RemoveOperation extends CollectionBackupAwareOperation {

    Data value;

    public RemoveOperation() {
    }

    public RemoveOperation(String name, CollectionProxyType proxyType, Data dataKey, int threadId, Data value) {
        super(name, proxyType, dataKey, threadId);
        this.value = value;
    }

    public void run() throws Exception {
        Collection<CollectionRecord> coll = getCollection();
        if (coll == null) {
            response = false;
            return;
        }
        CollectionRecord record = new CollectionRecord(isBinary() ? value : toObject(value));
        response = coll.remove(record);
        if (coll.isEmpty()) {
            removeCollection();
        }
    }

    public void afterRun() throws Exception {
        if (Boolean.TRUE.equals(response)) {
            publishEvent(EntryEventType.REMOVED, dataKey, value);
        }
    }

    public boolean shouldBackup() {
        return Boolean.TRUE.equals(response);
    }

    public Operation getBackupOperation() {
        return new RemoveBackupOperation(name, proxyType, dataKey, value);
    }

    public void onWaitExpire() {
        getResponseHandler().sendResponse(false);
    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        value.writeData(out);
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        value = IOUtil.readData(in);
    }
}
