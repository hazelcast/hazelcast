/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.vector.internal.impl.service;

import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationexecutor.OperationExecutor;
import com.hazelcast.vector.internal.impl.VectorCollectionOptimizationManager;
import com.hazelcast.vector.internal.impl.ops.VectorOptimizeWaitNotifyKey;

import java.util.concurrent.Semaphore;

import static com.hazelcast.internal.util.JVMUtil.OBJECT_HEADER_SIZE;
import static com.hazelcast.internal.util.JVMUtil.REFERENCE_COST_IN_BYTES;
import static com.hazelcast.vector.internal.impl.service.VectorCollectionServiceImpl.MAX_CONCURRENT_OPTIMIZE;

public class VectorCollectionOptimizationManagerImpl implements VectorCollectionOptimizationManager {
    public static final long FIXED_HEAP_BYTES_USED = OBJECT_HEADER_SIZE + 3L * REFERENCE_COST_IN_BYTES
            // optimizationWaitNotifyKeys array
            + OBJECT_HEADER_SIZE;

    private final NodeEngine nodeEngine;
    private final Semaphore optimizationPermits;
    private final VectorOptimizeWaitNotifyKey[] optimizationWaitNotifyKeys;

    public VectorCollectionOptimizationManagerImpl(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
        var maxConcurrentOptimize = nodeEngine.getProperties().getInteger(MAX_CONCURRENT_OPTIMIZE);
        optimizationPermits = new Semaphore(maxConcurrentOptimize);

        int threadCount = nodeEngine.getOperationService().getPartitionThreadCount();
        optimizationWaitNotifyKeys = new VectorOptimizeWaitNotifyKey[threadCount];
        for (int i = 0; i < threadCount; i++) {
            optimizationWaitNotifyKeys[i] = new VectorOptimizeWaitNotifyKey(i);
        }
    }

    @Override
    public boolean tryAcquireOptimizationPermit() {
        return optimizationPermits.tryAcquire();
    }

    @Override
    public boolean hasAvailableOptimizationPermit() {
        return optimizationPermits.availablePermits() > 0;
    }

    @Override
    public void releaseOptimizationPermit() {
        optimizationPermits.release();
    }

    @Override
    public VectorOptimizeWaitNotifyKey getOptimizePartitionWaitNotifyKey(int partitionId) {
        // wait key must be accessed only from single partition thread
        // (see comments in OperationParker).
        // So we have a notify key per partition thread for optimizations
        // to decrease number of different keys
        var threadId = getOperationExecutor().getPartitionThreadId(partitionId);
        return getOptimizeWaitNotifyKey(threadId);
    }

    @Override
    public VectorOptimizeWaitNotifyKey getOptimizeWaitNotifyKey(int threadId) {
        return optimizationWaitNotifyKeys[threadId];
    }

    private OperationExecutor getOperationExecutor() {
        return nodeEngine.getOperationService().getOperationExecutor();
    }

    @Override
    public long heapBytesUsed() {
        return FIXED_HEAP_BYTES_USED
                + optimizationWaitNotifyKeys.length * VectorOptimizeWaitNotifyKey.FIXED_HEAP_BYTES_USED;
    }
}
