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

package com.hazelcast.client.util;

import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.client.spi.ClientPartitionService;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.executor.impl.client.CancellationRequest;
import com.hazelcast.nio.Address;
import com.hazelcast.util.executor.DelegatingFuture;

import java.util.concurrent.CancellationException;
import java.util.concurrent.Future;

import static com.hazelcast.util.ExceptionUtil.rethrow;

/**
 * A DelegatingFuture that can cancel a Runnable/Callable that is executed by an {@link com.hazelcast.core.IExecutorService}.
 * It does this by sending a CancellationRequest to the remote owning member and then cancelling the running task.
 *
 * @param <V>
 */
public final class ClientCancellableDelegatingFuture<V> extends DelegatingFuture<V> {

    private final ClientContext context;
    private final String uuid;
    private final Address target;
    private final int partitionId;
    private volatile boolean cancelled;

    public ClientCancellableDelegatingFuture(ICompletableFuture future, ClientContext context,
                                             String uuid, Address target, int partitionId) {
        super(future, context.getSerializationService());
        this.context = context;
        this.uuid = uuid;
        this.target = target;
        this.partitionId = partitionId;
    }

    public ClientCancellableDelegatingFuture(ICompletableFuture future, ClientContext context,
                                             String uuid, Address target, int partitionId, V defaultValue) {
        super(future, context.getSerializationService(), defaultValue);
        this.context = context;
        this.uuid = uuid;
        this.target = target;
        this.partitionId = partitionId;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        if (isDone() || cancelled) {
            return false;
        }

        final Future f = invokeCancelRequest(mayInterruptIfRunning);
        try {
            final Boolean b = context.getSerializationService().toObject(f.get());
            if (b != null && b) {
                setError(new CancellationException());
                cancelled = true;
                return true;
            }
            return false;
        } catch (Exception e) {
            throw rethrow(e);
        } finally {
            setDone();
        }
    }

    private Future invokeCancelRequest(boolean mayInterruptIfRunning) {
        CancellationRequest request;
        Address address = target;
        if (target != null) {
            request = new CancellationRequest(uuid, target, mayInterruptIfRunning);
        } else {
            final ClientPartitionService partitionService = context.getPartitionService();
            address =  partitionService.getPartitionOwner(partitionId);
            request = new CancellationRequest(uuid, partitionId, mayInterruptIfRunning);
        }
        try {
            return context.getInvocationService().invokeOnTarget(request, address);
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    @Override
    public boolean isCancelled() {
        return cancelled;
    }
}
