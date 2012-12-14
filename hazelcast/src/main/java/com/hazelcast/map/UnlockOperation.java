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

import com.hazelcast.spi.ResponseHandler;
import com.hazelcast.nio.Data;

public class UnlockOperation extends TTLAwareOperation {

    public UnlockOperation(String name, Data dataKey) {
        super(name, dataKey);
    }

    public UnlockOperation() {
    }

    public void run() {
        // careful: Unlock cannot be scheduled.
        int partitionId = getPartitionId();
        ResponseHandler responseHandler = getResponseHandler();
        MapService mapService = (MapService) getService();
        PartitionContainer pc = mapService.getPartitionContainer(partitionId);
        MapPartition mapPartition = mapService.getMapPartition(partitionId, name);
        LockInfo lock = mapPartition.getLock(getKey());
        if (lock != null && lock.testLock(threadId, getCaller())) {
            if (lock.unlock(getCaller(), threadId)) {
                GenericBackupOperation backupOp = new GenericBackupOperation(name, dataKey, null, ttl);
                backupOp.setBackupOpType(GenericBackupOperation.BackupOpType.UNLOCK);
                int backupCount = mapPartition.getBackupCount();
//                getNodeEngine().sendAsyncBackups(MapService.MAP_SERVICE_NAME, backupOp, partitionId, backupCount);
            }
            responseHandler.sendResponse(Boolean.TRUE);
            if (!lock.isLocked()) {
                pc.onUnlock(lock, name, getKey());
            }
        } else {
            responseHandler.sendResponse(Boolean.FALSE);
        }
    }
}
