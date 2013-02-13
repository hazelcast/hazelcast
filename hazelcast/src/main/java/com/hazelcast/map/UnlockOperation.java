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
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Notifier;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.WaitNotifyKey;

public class UnlockOperation extends AbstractMapOperation implements BackupAwareOperation, Notifier {

    private transient Object keyObject;
    protected transient RecordStore recordStore;
    protected transient MapService mapService;
    protected transient boolean unlocked = false;

    public UnlockOperation(String name, Data dataKey) {
        super(name, dataKey);
    }

    public UnlockOperation() {
    }

    public void beforeRun() {
        mapService = getService();
        PartitionContainer pc = mapService.getPartitionContainer(getPartitionId());
        recordStore = pc.getRecordStore(name);
    }

    public void run() {
        unlocked = recordStore.unlock(dataKey, getCallerUuid(), threadId);
        mapService.getMapContainer(name).getMapOperationCounter().incrementOtherOperations();
    }

    @Override
    public Object getResponse() {
        return unlocked;
    }

    public boolean shouldBackup() {
        return unlocked;
    }

    public int getSyncBackupCount() {
        return mapService.getMapContainer(name).getBackupCount();
    }

    public int getAsyncBackupCount() {
        return mapService.getMapContainer(name).getAsyncBackupCount();
    }

    public Operation getBackupOperation() {
        return new UnlockBackupOperation(name, dataKey, getCallerUuid(), getThreadId());
    }

    public boolean shouldNotify() {
        return unlocked;
    }

    public WaitNotifyKey getNotifiedKey() {
        if (keyObject == null) {
            keyObject = getNodeEngine().toObject(dataKey);
        }
        return new MapWaitKey(getName(), keyObject, "lock");
    }

}
