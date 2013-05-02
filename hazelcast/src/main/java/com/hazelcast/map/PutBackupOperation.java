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

package com.hazelcast.map;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.BackupOperation;

public final class PutBackupOperation extends KeyBasedMapOperation implements BackupOperation, IdentifiedDataSerializable {

    private boolean unlockKey = false;

    public PutBackupOperation(String name, Data dataKey, Data dataValue, long ttl) {
        super(name, dataKey, dataValue, ttl);
    }

    public PutBackupOperation(String name, Data dataKey, Data dataValue, long ttl, boolean unlockKey) {
        super(name, dataKey, dataValue, ttl);
        this.unlockKey = unlockKey;
    }

    public PutBackupOperation() {
    }

    public void run() {
        Record record = recordStore.getRecords().get(dataKey);
        if (record == null) {
            record = mapService.createRecord(name, dataKey, dataValue, ttl, false);
            recordStore.getRecords().put(dataKey, record);
        } else {
            if (record instanceof DataRecord)
                ((DataRecord) record).setValue(dataValue);
            else if (record instanceof ObjectRecord)
                ((ObjectRecord) record).setValue(mapService.toObject(dataValue));
        }
        if(unlockKey)
            recordStore.forceUnlock(dataKey);
    }

    @Override
    public Object getResponse() {
        return Boolean.TRUE;
    }

    @Override
    public String toString() {
        return "PutBackupOperation{" + name + "}";
    }

    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    public int getId() {
        return MapDataSerializerHook.PUT_BACKUP;
    }
}
