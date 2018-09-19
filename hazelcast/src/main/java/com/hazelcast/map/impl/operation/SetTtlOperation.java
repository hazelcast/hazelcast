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
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.MutatingOperation;

import static com.hazelcast.map.impl.recordstore.RecordStore.DEFAULT_MAX_IDLE;

public class SetTtlOperation extends LockAwareOperation implements BackupAwareOperation, MutatingOperation {

    private transient boolean response;

    public SetTtlOperation() {

    }

    public SetTtlOperation(String name, Data dataKey, long ttl) {
        super(name, dataKey, ttl, DEFAULT_MAX_IDLE);
    }

    @Override
    public void onWaitExpire() {
        sendResponse(null);
    }

    @Override
    public void run() throws Exception {
        response = recordStore.setTtl(dataKey, ttl);
    }

    @Override
    public void afterRun() throws Exception {
        Record record = recordStore.getRecord(dataKey);
        if (record != null) {
            publishWanUpdate(dataKey, record.getValue());
            invalidateNearCache(dataKey);
        }
    }

    @Override
    public int getId() {
        return MapDataSerializerHook.SET_TTL;
    }

    @Override
    public Object getResponse() {
        return response;
    }

    @Override
    public boolean shouldBackup() {
        return mapContainer.getTotalBackupCount() > 0;
    }

    @Override
    public int getSyncBackupCount() {
        return mapContainer.getBackupCount();
    }

    @Override
    public int getAsyncBackupCount() {
        return mapContainer.getAsyncBackupCount();
    }

    @Override
    public Operation getBackupOperation() {
        return new SetTtlBackupOperation(name, dataKey, ttl);
    }
}
