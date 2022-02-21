/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.operationservice.BackupOperation;

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

    private List<Data> loadingSequence;
    private boolean includesExpirationTime;

    public PutFromLoadAllBackupOperation() {
        loadingSequence = Collections.emptyList();
    }

    public PutFromLoadAllBackupOperation(String name, List<Data> loadingSequence, boolean includesExpirationTime) {
        super(name);
        this.loadingSequence = loadingSequence;
        this.includesExpirationTime = includesExpirationTime;
    }

    @Override
    protected void runInternal() {
        final List<Data> keyValueSequence = this.loadingSequence;
        if (keyValueSequence == null || keyValueSequence.isEmpty()) {
            return;
        }
        for (int i = 0; i < keyValueSequence.size();) {
            final Data key = keyValueSequence.get(i++);
            final Data value = keyValueSequence.get(i++);
            final Object object = mapServiceContext.toObject(value);
            if (includesExpirationTime) {
                long expirationTime = (long) mapServiceContext.toObject(keyValueSequence.get(i++));
                recordStore.putFromLoadBackup(key, object, expirationTime);
            } else {
                recordStore.putFromLoadBackup(key, object);
            }
            // the following check is for the case when the putFromLoad does not put the data due to various reasons
            // one of the reasons may be size eviction threshold has been reached
            if (!recordStore.existInMemory(key)) {
                continue;
            }

            publishLoadAsWanUpdate(key, value);
        }
    }

    @Override
    protected void afterRunInternal() {
        evict(null);

        super.afterRunInternal();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeBoolean(this.includesExpirationTime);
        final List<Data> keyValueSequence = this.loadingSequence;
        final int size = keyValueSequence.size();
        out.writeInt(size);
        for (Data data : keyValueSequence) {
            IOUtil.writeData(out, data);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        this.includesExpirationTime = in.readBoolean();
        final int size = in.readInt();
        if (size < 1) {
            loadingSequence = Collections.emptyList();
        } else {
            final List<Data> tmpLoadingSequence = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                Data data = IOUtil.readData(in);
                tmpLoadingSequence.add(data);
            }
            loadingSequence = tmpLoadingSequence;
        }
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.PUT_FROM_LOAD_ALL_BACKUP;
    }
}
