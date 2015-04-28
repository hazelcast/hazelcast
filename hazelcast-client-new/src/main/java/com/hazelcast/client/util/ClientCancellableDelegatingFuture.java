/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.parameters.ExecutorServiceCancelOnAddressParameters;
import com.hazelcast.client.impl.protocol.parameters.ExecutorServiceCancelOnPartitionParameters;
import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.client.spi.impl.ClientInvocationFuture;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Address;
import com.hazelcast.util.EmptyStatement;
import com.hazelcast.util.executor.DelegatingFuture;

import java.util.concurrent.CancellationException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.util.ExceptionUtil.rethrow;

/**
 * A DelegatingFuture that can cancel a Runnable/Callable that is executed by an {@link com.hazelcast.core.IExecutorService}.
 * It does this by sending a CancellationRequest to the remote owning member and then cancelling the running task.
 *
 * @param <V> Type of returned object from the get method of this class.
 */
public final class ClientCancellableDelegatingFuture<V> extends DelegatingFuture<V> {

    private static final int INVOCATION_WAIT_TIMEOUT_SECONDS = 5;
    private final ClientContext context;
    private final String uuid;
    private final Address target;
    private final int partitionId;
    private final ILogger logger = Logger.getLogger(ClientCancellableDelegatingFuture.class);
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

        waitForRequestToBeSend();
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
        ClientInvocation clientInvocation;
        final HazelcastClientInstanceImpl client = (HazelcastClientInstanceImpl) context.getHazelcastInstance();
        if (target != null) {
            ClientMessage request = ExecutorServiceCancelOnAddressParameters.encode(uuid, target.getHost(),
                    target.getPort(), mayInterruptIfRunning);
            clientInvocation = new ClientInvocation(client, request, target);
        } else {
            ClientMessage request =
                    ExecutorServiceCancelOnPartitionParameters.encode(uuid, partitionId, mayInterruptIfRunning);
            clientInvocation = new ClientInvocation(client, request, partitionId);
        }

        try {
            return clientInvocation.invoke();
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    private void waitForRequestToBeSend() {
        final ICompletableFuture future = getFuture();

        final ClientInvocationFuture clientCallFuture = (ClientInvocationFuture) future;
        ClientInvocation invocation = clientCallFuture.getInvocation();

        int timeoutSeconds = INVOCATION_WAIT_TIMEOUT_SECONDS;
        while (!invocation.isInvoked()) {
            if (timeoutSeconds-- == 0) {
                logger.warning("Cancel is failed because runnable/callable never send to remote !");
                break;
            }
            try {
                Thread.sleep(TimeUnit.SECONDS.toMillis(1));
            } catch (InterruptedException ignored) {
                EmptyStatement.ignore(ignored);
            }
        }

    }

    @Override
    public boolean isCancelled() {
        return cancelled;
    }
}
