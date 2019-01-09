/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.concurrent.semaphore.operations;

import com.hazelcast.concurrent.semaphore.SemaphoreContainer;
import com.hazelcast.concurrent.semaphore.SemaphoreDataSerializerHook;
import com.hazelcast.concurrent.semaphore.SemaphoreService;
import com.hazelcast.concurrent.semaphore.SemaphoreWaitNotifyKey;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Notifier;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.WaitNotifyKey;
import com.hazelcast.spi.partition.IPartition;
import com.hazelcast.spi.partition.IPartitionService;

import java.io.IOException;

public class SemaphoreDetachMemberOperation extends SemaphoreBackupAwareOperation implements Notifier {

    private String detachedMemberUuid;

    public SemaphoreDetachMemberOperation() {
    }

    public SemaphoreDetachMemberOperation(String name, String detachedMemberUuid) {
        super(name, -1);
        this.detachedMemberUuid = detachedMemberUuid;
    }

    @Override
    public void run() throws Exception {
        SemaphoreService service = getService();
        if (service.containsSemaphore(name)) {
            SemaphoreContainer semaphoreContainer = service.getSemaphoreContainer(name);
            response = semaphoreContainer.detachAll(detachedMemberUuid);
        }

        ILogger logger = getLogger();
        if (logger.isFineEnabled()) {
            logger.fine("Removing permits attached to " + detachedMemberUuid + ". Result: " + response);
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
    public int getAsyncBackupCount() {
        int syncBackupCount = super.getSyncBackupCount();
        int asyncBackupCount = super.getAsyncBackupCount();
        return syncBackupCount + asyncBackupCount;
    }

    @Override
    public int getSyncBackupCount() {
        return 0;
    }

    @Override
    public Operation getBackupOperation() {
        return new SemaphoreDetachMemberBackupOperation(name, detachedMemberUuid);
    }

    @Override
    public boolean shouldNotify() {
        return Boolean.TRUE.equals(response);
    }

    @Override
    public WaitNotifyKey getNotifiedKey() {
        return new SemaphoreWaitNotifyKey(name, "acquire");
    }

    @Override
    public int getId() {
        return SemaphoreDataSerializerHook.DETACH_MEMBER_OPERATION;
    }

    @Override
    public void writeInternal(ObjectDataOutput out) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void readInternal(ObjectDataInput in) throws IOException {
        throw new UnsupportedOperationException();
    }
}
