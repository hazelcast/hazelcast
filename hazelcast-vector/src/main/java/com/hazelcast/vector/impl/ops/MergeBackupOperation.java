/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.vector.impl.ops;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.impl.SerializationUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.vector.impl.VectorCollectionSerializerHook;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.List;

public class MergeBackupOperation extends PutAllBackupOperation {

    private List<Data> removedKeys;

    public MergeBackupOperation() {
    }

    public MergeBackupOperation(String name, VectorEntries vectorEntries, @Nonnull List<Data> removedKeys) {
        super(name, vectorEntries);
        this.removedKeys = removedKeys;
    }

    @Override
    public void run() throws Exception {
        putAll();

        if (removedKeys != null) {
            for (var key : removedKeys) {
                storage.delete(key);
            }
        }

        if (backupOpAfterRun != null) {
            // we were waiting - send backup acks
            backupOpAfterRun.accept(this);
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        SerializationUtil.writeList(removedKeys, out);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        removedKeys = SerializationUtil.readList(in);
    }

    @Override
    public int getClassId() {
        return VectorCollectionSerializerHook.MERGE_BACKUP_OPERATION;
    }
}
