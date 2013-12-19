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

package com.hazelcast.collection;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupOperation;

import java.io.IOException;

/**
 * @ali 9/4/13
 */
public class CollectionAddBackupOperation extends CollectionOperation implements BackupOperation {

    private long itemId;

    private Data value;

    public CollectionAddBackupOperation() {
    }

    public CollectionAddBackupOperation(String name, long itemId, Data value) {
        super(name);
        this.itemId = itemId;
        this.value = value;
    }

    public int getId() {
        return CollectionDataSerializerHook.COLLECTION_ADD_BACKUP;
    }

    public void beforeRun() throws Exception {
    }

    public void run() throws Exception {
        getOrCreateContainer().addBackup(itemId, value);
    }

    public void afterRun() throws Exception {

    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(itemId);
        value.writeData(out);
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        itemId = in.readLong();
        value = new Data();
        value.readData(in);
    }
}
