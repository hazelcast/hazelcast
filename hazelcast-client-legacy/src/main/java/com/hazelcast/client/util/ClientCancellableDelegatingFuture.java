/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.client.ClientRequest;
import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.client.spi.impl.ClientInvocationFuture;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.executor.impl.client.CancellationRequest;
import com.hazelcast.nio.Address;
import com.hazelcast.util.executor.DelegatingFuture;

import java.util.concurrent.CancellationException;

import static com.hazelcast.util.ExceptionUtil.rethrow;

/**
 * A DelegatingFuture that can cancel a Runnable/Callable that is executed by an {@link com.hazelcast.core.IExecutorService}.
 * It does this by sending a CancellationRequest to the remote owning member and then cancelling the running task.
 *
 * @param <V> Type of returned object from the get method of this class.
 */
public final class ClientCancellableDelegatingFuture<V> extends DelegatingFuture<V> {

    private final ClientContext context;
    private final String uuid;
    private final Address target;
    private final int partitionId;
    private volatile boolean cancelled;

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

        Boolean b = false;
        try {
            b = invokeCancelRequest(mayInterruptIfRunning);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        try {
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

    private Boolean invokeCancelRequest(boolean mayInterruptIfRunning) throws InterruptedException {
        waitForRequestToBeSend();

        ClientInvocation clientInvocation;
        final HazelcastClientInstanceImpl client = (HazelcastClientInstanceImpl) context.getHazelcastInstance();
        if (target != null) {
            CancellationRequest request = new CancellationRequest(uuid, target, mayInterruptIfRunning);
            clientInvocation = new ClientInvocation(client, request, target);
        } else {
            ClientRequest request = new CancellationRequest(uuid, partitionId, mayInterruptIfRunning);
            clientInvocation = new ClientInvocation(client, request, partitionId);
        }

        try {
            ClientInvocationFuture f = clientInvocation.invoke();
            return context.getSerializationService().toObject(f.get());
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    private void waitForRequestToBeSend() throws InterruptedException {
        ICompletableFuture future = getFuture();
        ClientInvocationFuture clientCallFuture = (ClientInvocationFuture) future;
        clientCallFuture.getInvocation().getSendConnectionOrWait();
    }

    @Override
    public boolean isCancelled() {
        return cancelled;
    }
}
