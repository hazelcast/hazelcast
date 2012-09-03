/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

package com.hazelcast.impl.map;

import com.hazelcast.impl.spi.ResponseHandler;
import com.hazelcast.nio.Data;

public class LockOperation extends LockAwareOperation {

    public static final long DEFAULT_LOCK_TTL = 5 * 60 * 1000;

    public LockOperation(String name, Data dataKey) {
        this(name, dataKey, DEFAULT_LOCK_TTL);
    }

    public LockOperation(String name, Data dataKey, long ttl) {
        super(name, dataKey);
        this.ttl = ttl;
    }

    public LockOperation() {
    }

    public void doRun() {
        int partitionId = getPartitionId();
        ResponseHandler responseHandler = getResponseHandler();
        MapService mapService = (MapService) getService();
        PartitionContainer pc = mapService.getPartitionContainer(partitionId);
        MapPartition mapPartition = mapService.getMapPartition(partitionId, name);
        LockInfo lock = mapPartition.getOrCreateLock(getKey());
        boolean locked = lock.lock(getCaller(), threadId, ttl);
        if (locked) {
            GenericBackupOperation backupOp = new GenericBackupOperation(name, dataKey, null, ttl, pc.incrementAndGetVersion());
            backupOp.setBackupOpType(GenericBackupOperation.BackupOpType.LOCK);
            int backupCount = mapPartition.getBackupCount();
            getNodeService().sendBackups(MapService.MAP_SERVICE_NAME, backupOp, partitionId, backupCount);
        }
        responseHandler.sendResponse(locked);
    }
}
