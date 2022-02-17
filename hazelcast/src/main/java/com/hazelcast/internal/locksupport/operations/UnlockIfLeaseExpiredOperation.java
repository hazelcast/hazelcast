/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.locksupport.operations;

import com.hazelcast.internal.locksupport.LockDataSerializerHook;
import com.hazelcast.internal.locksupport.LockStoreImpl;
import com.hazelcast.internal.services.ObjectNamespace;
import com.hazelcast.logging.ILogger;
import com.hazelcast.cluster.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.SplitBrainProtectionCheckAwareOperation;
import com.hazelcast.internal.partition.IPartition;
import com.hazelcast.internal.partition.IPartitionService;

import java.io.IOException;

public final class UnlockIfLeaseExpiredOperation extends UnlockOperation implements SplitBrainProtectionCheckAwareOperation {

    private int version;

    public UnlockIfLeaseExpiredOperation() {
    }

    public UnlockIfLeaseExpiredOperation(ObjectNamespace namespace, Data key, int version) {
        super(namespace, key, -1, true);
        this.version = version;
    }

    @Override
    public void run() {
        LockStoreImpl lockStore = getLockStore();
        int lockVersion = lockStore.getVersion(key);
        ILogger logger = getLogger();
        if (version == lockVersion) {
            if (logger.isFinestEnabled()) {
                logger.finest("Releasing a lock owned by " + lockStore.getOwnerInfo(key) + " after lease timeout!");
            }
            forceUnlock();
        } else {
            if (logger.isFinestEnabled()) {
                logger.finest("Won't unlock since lock version is not matching expiration version: "
                        + lockVersion + " vs " + version);
            }
        }
    }

    /**
     * This operation runs on both primary and backup
     * If it is running on backup we should not send a backup operation
     *
     * @return
     */
    @Override
    public boolean shouldBackup() {
        NodeEngine nodeEngine = getNodeEngine();
        IPartitionService partitionService = nodeEngine.getPartitionService();
        Address thisAddress = nodeEngine.getThisAddress();
        IPartition partition = partitionService.getPartition(getPartitionId());
        if (!thisAddress.equals(partition.getOwnerOrNull())) {
            return false;
        }
        return super.shouldBackup();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(version);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        version = in.readInt();
    }

    @Override
    public int getClassId() {
        return LockDataSerializerHook.UNLOCK_IF_LEASE_EXPIRED;
    }

    @Override
    public boolean shouldCheckSplitBrainProtection() {
        return false;
    }
}
