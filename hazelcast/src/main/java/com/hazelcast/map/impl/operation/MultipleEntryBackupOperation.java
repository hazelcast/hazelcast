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

package com.hazelcast.map.impl.operation;

import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupOperation;

import java.io.IOException;
import java.util.Set;

import static com.hazelcast.map.impl.operation.EntryOperator.operator;
import static com.hazelcast.util.SetUtil.createLinkedHashSet;

public class MultipleEntryBackupOperation extends AbstractMultipleEntryBackupOperation implements BackupOperation {

    private Set<Data> keys;

    public MultipleEntryBackupOperation() {
    }

    public MultipleEntryBackupOperation(String name, Set<Data> keys, EntryBackupProcessor backupProcessor) {
        super(name, backupProcessor);
        this.keys = keys;
    }

    @Override
    public void run() throws Exception {
        EntryOperator operator = operator(this, backupProcessor, getPredicate(), true);
        for (Data key : keys) {
            operator.operateOnKey(key).doPostOperateOps();
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        backupProcessor = in.readObject();
        int size = in.readInt();
        keys = createLinkedHashSet(size);
        for (int i = 0; i < size; i++) {
            Data key = in.readData();
            keys.add(key);
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(backupProcessor);
        out.writeInt(keys.size());
        for (Data key : keys) {
            out.writeData(key);
        }
    }

    @Override
    public Object getResponse() {
        return true;
    }

    @Override
    public int getId() {
        return MapDataSerializerHook.MULTIPLE_ENTRY_BACKUP;
    }
}
