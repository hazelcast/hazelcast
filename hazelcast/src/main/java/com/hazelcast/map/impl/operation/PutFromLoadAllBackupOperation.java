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

package com.hazelcast.map.impl.operation;

import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupOperation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Runs on backups.
 *
 * @see PutFromLoadAllOperation
 */
public class PutFromLoadAllBackupOperation extends MapOperation implements BackupOperation {

    private List<Data> keyValueSequence;

    public PutFromLoadAllBackupOperation() {
        keyValueSequence = Collections.emptyList();
    }

    public PutFromLoadAllBackupOperation(String name, List<Data> keyValueSequence) {
        super(name);
        this.keyValueSequence = keyValueSequence;
    }

    @Override
    public void run() throws Exception {
        final List<Data> keyValueSequence = this.keyValueSequence;
        if (keyValueSequence == null || keyValueSequence.isEmpty()) {
            return;
        }
        for (int i = 0; i < keyValueSequence.size(); i += 2) {
            final Data key = keyValueSequence.get(i);
            final Data value = keyValueSequence.get(i + 1);
            final Object object = mapServiceContext.toObject(value);
            recordStore.putFromLoadBackup(key, object);
            // the following check is for the case when the putFromLoad does not put the data due to various reasons
            // one of the reasons may be size eviction threshold has been reached
            if (!recordStore.existInMemory(key)) {
                continue;
            }

            publishWanUpdate(key, value);
        }
    }

    @Override
    public void afterRun() throws Exception {
        evict(null);

        super.afterRun();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        final List<Data> keyValueSequence = this.keyValueSequence;
        final int size = keyValueSequence.size();
        out.writeInt(size);
        for (Data data : keyValueSequence) {
            out.writeData(data);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        final int size = in.readInt();
        if (size < 1) {
            keyValueSequence = Collections.emptyList();
        } else {
            final List<Data> tmpKeyValueSequence = new ArrayList<Data>(size);
            for (int i = 0; i < size; i++) {
                Data data = in.readData();
                tmpKeyValueSequence.add(data);
            }
            keyValueSequence = tmpKeyValueSequence;
        }
    }

    @Override
    public int getId() {
        return MapDataSerializerHook.PUT_FROM_LOAD_ALL_BACKUP;
    }
}
