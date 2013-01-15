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
import com.hazelcast.spi.*;
import com.hazelcast.spi.impl.AbstractNamedKeyBasedOperation;

public class ForceUnlockOperation extends AbstractNamedKeyBasedOperation implements BackupAwareOperation, Notifier {

    PartitionContainer pc;
    ResponseHandler responseHandler;
    RecordStore recordStore;
    MapService mapService;
    NodeEngine nodeEngine;
    boolean unlocked = false;

    public ForceUnlockOperation(String name, Data dataKey) {
        super(name, dataKey);
    }

    public ForceUnlockOperation() {
    }

    protected void init() {
        responseHandler = getResponseHandler();
        mapService = getService();
        nodeEngine = getNodeEngine();
        pc = mapService.getPartitionContainer(getPartitionId());
        recordStore = pc.getRecordStore(name);
    }

    public void beforeRun() {
        init();
    }

    public void run() {
        doOp();
    }

    public void doOp() {
        unlocked = recordStore.forceUnlock(dataKey);
    }

    public boolean shouldBackup() {
        return unlocked;
    }

    public int getSyncBackupCount() {
        return mapService.getMapInfo(name).getBackupCount();
    }

    public int getAsyncBackupCount() {
        return mapService.getMapInfo(name).getAsyncBackupCount();
    }

    public Operation getBackupOperation() {
        GenericBackupOperation backupOp = new GenericBackupOperation(name, dataKey, null, -1);
        backupOp.setBackupOpType(GenericBackupOperation.BackupOpType.UNLOCK);
        return backupOp;
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
