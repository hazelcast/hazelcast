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

package com.hazelcast.collection.list;

import com.hazelcast.collection.CollectionDataSerializerHook;
import com.hazelcast.collection.CollectionOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupOperation;
import java.io.IOException;

public class ListSetBackupOperation extends CollectionOperation implements BackupOperation {

    private long oldItemId;
    private long itemId;
    private Data value;

    public ListSetBackupOperation() {
    }

    public ListSetBackupOperation(String name, long oldItemId, long itemId, Data value) {
        super(name);
        this.oldItemId = oldItemId;
        this.itemId = itemId;
        this.value = value;
    }

    @Override
    public int getId() {
        return CollectionDataSerializerHook.LIST_SET_BACKUP;
    }

    @Override
    public void beforeRun() throws Exception {
    }

    @Override
    public void run() throws Exception {
        getOrCreateListContainer().setBackup(oldItemId, itemId, value);
    }

    @Override
    public void afterRun() throws Exception {
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(oldItemId);
        out.writeLong(itemId);
        out.writeData(value);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        oldItemId = in.readLong();
        itemId = in.readLong();
        value = in.readData();
    }
}
