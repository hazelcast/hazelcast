/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.spi.*;
import com.hazelcast.nio.Data;

import static com.hazelcast.nio.IOUtil.toObject;

public class UnlockOperation extends TTLAwareOperation implements BackupAwareOperation, Notifier {

    PartitionContainer pc;
    ResponseHandler responseHandler;
    DefaultRecordStore mapPartition;
    MapService mapService;
    NodeEngine nodeEngine;
    boolean unlocked = false;

    public UnlockOperation(String name, Data dataKey) {
        super(name, dataKey);
    }

    public UnlockOperation() {
    }

    protected void init() {
        responseHandler = getResponseHandler();
        mapService = getService();
        nodeEngine = getNodeEngine();
        pc = mapService.getPartitionContainer(getPartitionId());
        mapPartition = pc.getMapPartition(name);
    }

    public void beforeRun() {
        init();
    }

    public void run() {
        doOp();
    }

    public void doOp() {
        LockInfo lock = mapPartition.getLock(getKey());
        if (lock == null)
            return;
        if (lock.testLock(threadId, getCaller())) {
            if (lock.unlock(getCaller(), threadId)) {
                unlocked = true;
            }
        }
    }

    public boolean shouldBackup() {
        return unlocked;
    }

    public int getSyncBackupCount() {
        return mapPartition.getBackupCount();
    }

    public int getAsyncBackupCount() {
        return mapPartition.getAsyncBackupCount();
    }

    public Operation getBackupOperation() {
        GenericBackupOperation backupOp = new GenericBackupOperation(name, dataKey, null, ttl);
        backupOp.setBackupOpType(GenericBackupOperation.BackupOpType.UNLOCK);
        return backupOp;
    }

    public boolean shouldNotify() {
        return unlocked;
    }

    public Object getNotifiedKey() {
        if (keyObject == null) {
            keyObject = toObject(dataKey);
        }
        return new MapWaitKey(getName(), keyObject, "lock");
    }
}
