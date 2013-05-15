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

package com.hazelcast.collection.operations;

import com.hazelcast.collection.CollectionDataSerializerHook;
import com.hazelcast.collection.CollectionProxyId;
import com.hazelcast.collection.CollectionRecord;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupOperation;

import java.io.IOException;
import java.util.List;

/**
 * @ali 1/17/13
 */
public class SetBackupOperation extends CollectionKeyBasedOperation implements BackupOperation {

    int index;
    long recordId;
    Data value;

    public SetBackupOperation() {
    }

    public SetBackupOperation(CollectionProxyId proxyId, Data dataKey, long recordId, int index, Data value) {
        super(proxyId, dataKey);
        this.index = index;
        this.recordId = recordId;
        this.value = value;
    }

    public void run() throws Exception {
        CollectionRecord record = new CollectionRecord(recordId, isBinary() ? value : toObject(value));
        List<CollectionRecord> list = (List<CollectionRecord>) getOrCreateCollectionWrapper().getCollection();
        list.set(index, record);
        response = true;
    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(index);
        out.writeLong(recordId);
        value.writeData(out);
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        index = in.readInt();
        recordId = in.readLong();
        value = new Data();
        value.readData(in);
    }

    public int getId() {
        return CollectionDataSerializerHook.SET_BACKUP;
    }
}
