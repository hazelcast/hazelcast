/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class PutAllDeleteAllBackupOperation extends PutAllBackupOperation {

    private Set<Data> deletions;

    public PutAllDeleteAllBackupOperation(String name, List<Object> dataKeyDataValueRecord, Set<Data> deletions,
                                          boolean disableWanReplicationEvent) {
        super(name, dataKeyDataValueRecord, disableWanReplicationEvent);
        this.deletions = deletions;
    }

    public PutAllDeleteAllBackupOperation() {
    }

    @Override
    protected void runInternal() {
        super.runInternal();
        if (deletions != null) {
            Iterator<Data> it = deletions.iterator();
            while (it.hasNext()) {
                Data deletion = it.next();
                removeBackup(deletion);
                it.remove();
            }
        }
    }

    private void removeBackup(Data key) {
        recordStore.removeBackup(key, getCallerProvenance());
        publishWanRemove(key);
        evict(key);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(deletions.size());
        for (Data data : deletions) {
            IOUtil.writeData(out, data);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int size = in.readInt();
        deletions = new HashSet<>(size);
        for (int i = 0; i < size; i++) {
            deletions.add(IOUtil.readData(in));
        }
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.PUT_ALL_DELETE_ALL_BACKUP;
    }
}
