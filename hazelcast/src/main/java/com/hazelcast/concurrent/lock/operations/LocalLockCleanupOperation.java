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

package com.hazelcast.concurrent.lock.operations;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.InternalPartition;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Notifier;
import com.hazelcast.spi.ObjectNamespace;

import java.io.IOException;

public class LocalLockCleanupOperation extends UnlockOperation implements Notifier, BackupAwareOperation {

    public LocalLockCleanupOperation() {
    }

    public LocalLockCleanupOperation(ObjectNamespace namespace, Data key, long threadId) {
        super(namespace, key, threadId, true);
    }

    @Override
    public boolean shouldBackup() {
        final NodeEngine nodeEngine = getNodeEngine();
        InternalPartitionService partitionService = nodeEngine.getPartitionService();
        InternalPartition partition = partitionService.getPartition(getPartitionId());
        return nodeEngine.getThisAddress().equals(partition.getOwnerOrNull())
                && Boolean.TRUE.equals(response);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        throw new UnsupportedOperationException();
    }
}
