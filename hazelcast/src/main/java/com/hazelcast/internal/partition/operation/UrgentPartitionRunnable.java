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

package com.hazelcast.internal.partition.operation;

import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.operationservice.UrgentSystemOperation;

import java.util.concurrent.Callable;

final class UrgentPartitionRunnable<T>
        implements PartitionSpecificRunnable, UrgentSystemOperation {

    final InternalCompletableFuture<T> future = new InternalCompletableFuture<>();
    private final int partitionId;
    private final Callable<T> callable;

    UrgentPartitionRunnable(int partitionId, Runnable runnable) {
        this.partitionId = partitionId;
        this.callable = () -> {
            runnable.run();
            return null;
        };
    }

    UrgentPartitionRunnable(int partitionId, Callable callable) {
        this.partitionId = partitionId;
        this.callable = callable;
    }

    @Override
    public int getPartitionId() {
        return partitionId;
    }

    @Override
    public void run() {
        try {
            future.complete(callable.call());
        } catch (Exception e) {
            future.completeExceptionally(e);
        }
    }
}
