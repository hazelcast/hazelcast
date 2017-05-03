/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.concurrent.lock.operations;

import com.hazelcast.concurrent.lock.LockResource;
import com.hazelcast.concurrent.lock.LockStoreImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Notifier;
import com.hazelcast.spi.ObjectNamespace;
import com.hazelcast.spi.partition.IPartition;
import com.hazelcast.spi.partition.IPartitionService;

import java.io.IOException;

public class LocalLockCleanupOperation extends UnlockOperation implements Notifier, BackupAwareOperation {

    private final String uuid;

    /**
     * This constructor should not be used to obtain an instance of this class; it exists to fulfill IdentifiedDataSerializable
     * coding conventions.
     */
    public LocalLockCleanupOperation() {
        uuid = "";
    }

    public LocalLockCleanupOperation(ObjectNamespace namespace, Data key, String uuid) {
        super(namespace, key, -1, true);
        this.uuid = uuid;
    }

    @Override
    public void run() throws Exception {
        LockStoreImpl lockStore = getLockStore();
        LockResource lock = lockStore.getLock(key);
        if (uuid.equals(lock.getOwner())) {
            ILogger logger = getLogger();
            if (logger.isFinestEnabled()) {
                logger.finest(
                        "Unlocking lock owned by UUID: " + uuid + ", thread ID: " + lock.getThreadId() + ", count: "
                                + lock.getLockCount());
            }
            response = lockStore.forceUnlock(key);
        }
    }

    @Override
    public boolean shouldBackup() {
        final NodeEngine nodeEngine = getNodeEngine();
        IPartitionService partitionService = nodeEngine.getPartitionService();
        IPartition partition = partitionService.getPartition(getPartitionId());
        return partition.isLocal() && Boolean.TRUE.equals(response);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getId() {
        throw new UnsupportedOperationException("LocalLockCleanupOperation is local only.");
    }
}
