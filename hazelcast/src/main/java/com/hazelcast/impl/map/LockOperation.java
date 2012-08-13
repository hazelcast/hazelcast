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

import com.hazelcast.impl.spi.AbstractNamedKeyBasedOperation;
import com.hazelcast.impl.spi.OperationContext;
import com.hazelcast.impl.spi.ResponseHandler;
import com.hazelcast.nio.Data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class LockOperation extends AbstractNamedKeyBasedOperation {

    public static final long DEFAULT_LOCK_TTL = 60 * 1000;

    long ttl = DEFAULT_LOCK_TTL; // how long should the lock live?

    public LockOperation(String name, Data dataKey) {
        super(name, dataKey);
    }

    public LockOperation(String name, Data dataKey, long ttl) {
        super(name, dataKey);
        this.ttl = ttl;
    }

    public LockOperation() {
    }

    public void run() {
        OperationContext context = getOperationContext();
        ResponseHandler responseHandler = context.getResponseHandler();
        MapService mapService = (MapService) context.getService();
        MapPartition mapPartition = mapService.getMapPartition(context.getPartitionId(), name);
        LockInfo lock = mapPartition.getOrCreateLock(getKey());
        if (lock.testLock(threadId, context.getCaller())) {
            boolean locked = lock.lock(context.getCaller(), threadId, ttl);
            if (locked) {
                GenericBackupOperation backupOp = new GenericBackupOperation(name, dataKey, ttl);
                backupOp.setBackupOpType(GenericBackupOperation.BackupOpType.LOCK);
                int partitionId = context.getPartitionId();
                int backupCount = mapPartition.getBackupCount();
                try {
                    context.getNodeService().takeBackups(MapService.MAP_SERVICE_NAME, backupOp, partitionId, backupCount, 60);
                } catch (Exception ignored) {
                }
            }
            responseHandler.sendResponse(locked);
        } else {
            mapPartition.schedule(LockOperation.this);
        }
    }

    @Override
    public void writeData(DataOutput out) throws IOException {
        super.writeData(out);
        out.writeLong(ttl);
    }

    @Override
    public void readData(DataInput in) throws IOException {
        super.readData(in);
        ttl = in.readLong();
    }
}
