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

import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.operationservice.BackupOperation;

import java.io.IOException;

public class RemoveBackupOperation extends KeyBasedMapOperation implements BackupOperation {

    private boolean disableWanReplicationEvent;

    public RemoveBackupOperation() {
    }

    public RemoveBackupOperation(String name, Data dataKey,
                                 boolean disableWanReplicationEvent) {
        super(name, dataKey);
        this.disableWanReplicationEvent = disableWanReplicationEvent;
    }

    @Override
    protected void runInternal() {
        recordStore.removeBackup(dataKey, getCallerProvenance());
    }

    @Override
    protected boolean disableWanReplicationEvent() {
        return disableWanReplicationEvent;
    }

    @Override
    protected void afterRunInternal() {
        publishWanRemove(dataKey);
        evict(dataKey);

        super.afterRunInternal();
    }

    @Override
    public Object getResponse() {
        return Boolean.TRUE;
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.REMOVE_BACKUP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeBoolean(disableWanReplicationEvent);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        disableWanReplicationEvent = in.readBoolean();
    }
}
