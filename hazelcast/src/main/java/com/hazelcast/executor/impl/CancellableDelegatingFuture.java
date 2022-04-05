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

package com.hazelcast.executor.impl;

import com.hazelcast.executor.impl.operations.CancellationOperation;
import com.hazelcast.cluster.Address;
import com.hazelcast.spi.impl.DelegatingCompletableFuture;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.InvocationBuilder;
import com.hazelcast.spi.impl.operationservice.OperationService;

import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Future;

import static com.hazelcast.internal.util.ExceptionUtil.rethrow;

final class CancellableDelegatingFuture<V> extends DelegatingCompletableFuture<V> {

    public static final int CANCEL_TRY_COUNT = 50;
    public static final int CANCEL_TRY_PAUSE_MILLIS = 250;

    private final NodeEngine nodeEngine;
    private final UUID uuid;
    private final int partitionId;
    private final Address target;

    CancellableDelegatingFuture(InternalCompletableFuture future, NodeEngine nodeEngine, UUID uuid, int partitionId) {
        super(nodeEngine.getSerializationService(), future);
        this.nodeEngine = nodeEngine;
        this.uuid = uuid;
        this.partitionId = partitionId;
        this.target = null;
    }

    CancellableDelegatingFuture(InternalCompletableFuture future, NodeEngine nodeEngine, UUID uuid, Address target) {
        super(nodeEngine.getSerializationService(), future);
        this.nodeEngine = nodeEngine;
        this.uuid = uuid;
        this.target = target;
        this.partitionId = -1;
    }

    CancellableDelegatingFuture(InternalCompletableFuture future, V defaultValue, NodeEngine nodeEngine,
                                UUID uuid, int partitionId) {
        super(nodeEngine.getSerializationService(), future, defaultValue);
        this.nodeEngine = nodeEngine;
        this.uuid = uuid;
        this.partitionId = partitionId;
        this.target = null;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        if (isDone()) {
            return false;
        }

        Future<Boolean> f = invokeCancelOperation(mayInterruptIfRunning);
        boolean cancelSuccessful = false;
        try {
            cancelSuccessful = f.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            throw rethrow(e);
        }

        completeExceptionally(new CancellationException());
        return cancelSuccessful;
    }

    private Future<Boolean> invokeCancelOperation(boolean mayInterruptIfRunning) {
        CancellationOperation op = new CancellationOperation(uuid, mayInterruptIfRunning);
        OperationService opService = nodeEngine.getOperationService();
        InvocationBuilder builder;
        if (partitionId > -1) {
            builder = opService.createInvocationBuilder(DistributedExecutorService.SERVICE_NAME, op, partitionId);
        } else {
            builder = opService.createInvocationBuilder(DistributedExecutorService.SERVICE_NAME, op, target);
        }
        builder.setTryCount(CANCEL_TRY_COUNT).setTryPauseMillis(CANCEL_TRY_PAUSE_MILLIS);
        return builder.invoke();
    }

}
