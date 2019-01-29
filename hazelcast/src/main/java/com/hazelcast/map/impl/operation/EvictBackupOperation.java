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
import com.hazelcast.util.Clock;

import java.io.IOException;

public class EvictBackupOperation extends KeyBasedMapOperation implements BackupOperation {

    protected boolean unlockKey;
    protected boolean disableWanReplicationEvent;

    public EvictBackupOperation() {
    }

    public EvictBackupOperation(String name, Data dataKey) {
        super(name, dataKey);
    }

    public EvictBackupOperation(String name, Data dataKey, boolean unlockKey) {
        super(name, dataKey);
        this.unlockKey = unlockKey;
    }

    public EvictBackupOperation(String name, Data dataKey, boolean unlockKey, boolean disableWanReplicationEvent) {
        super(name, dataKey);
        this.unlockKey = unlockKey;
        this.disableWanReplicationEvent = disableWanReplicationEvent;
    }

    @Override
    public void run() {
        recordStore.evict(dataKey, true);
        if (unlockKey) {
            recordStore.forceUnlock(dataKey);
        }
    }

    @Override
    public void afterRun() throws Exception {
        if (!disableWanReplicationEvent && mapContainer.isWanReplicationEnabled()) {
            mapEventPublisher.publishWanReplicationRemoveBackup(name, dataKey, Clock.currentTimeMillis());
        }
    }

    @Override
    public Object getResponse() {
        return Boolean.TRUE;
    }

    @Override
    public int getId() {
        return MapDataSerializerHook.EVICT_BACKUP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeBoolean(unlockKey);
        out.writeBoolean(disableWanReplicationEvent);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        unlockKey = in.readBoolean();
        disableWanReplicationEvent = in.readBoolean();
    }

}
