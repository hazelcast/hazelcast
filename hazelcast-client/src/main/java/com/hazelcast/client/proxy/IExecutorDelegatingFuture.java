/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.proxy;

import com.hazelcast.client.impl.clientside.ClientMessageDecoder;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ExecutorServiceCancelOnAddressCodec;
import com.hazelcast.client.impl.protocol.codec.ExecutorServiceCancelOnPartitionCodec;
import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.client.spi.impl.ClientInvocationFuture;
import com.hazelcast.client.util.ClientDelegatingFuture;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.nio.Address;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;

import static com.hazelcast.util.ExceptionUtil.rethrow;

/**
 * An Abstract DelegatingFuture that can cancel a Runnable/Callable that is executed by an
 * {@link com.hazelcast.core.IExecutorService}.
 * It does this by sending a Cancellation Request to the remote owning member and then cancelling the running task.
 *
 * @param <V> Type of returned object from the get method of this class.
 */
public final class IExecutorDelegatingFuture<V> extends ClientDelegatingFuture<V> {

    private final ClientContext context;
    private final String uuid;
    private final Address target;
    private final int partitionId;
    private final String objectName;

    IExecutorDelegatingFuture(ClientInvocationFuture future, ClientContext context,
                              String uuid, V defaultValue,
                              ClientMessageDecoder resultDecoder, String objectName, Address address) {
        super(future, context.getSerializationService(), resultDecoder, defaultValue);
        this.context = context;
        this.uuid = uuid;
        this.partitionId = -1;
        this.objectName = objectName;
        this.target = address;
    }

    IExecutorDelegatingFuture(ClientInvocationFuture future, ClientContext context,
                              String uuid, V defaultValue,
                              ClientMessageDecoder resultDecoder, String objectName, int partitionId) {
        super(future, context.getSerializationService(), resultDecoder, defaultValue);
        this.context = context;
        this.uuid = uuid;
        this.partitionId = partitionId;
        this.objectName = objectName;
        this.target = null;

    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        if (isDone()) {
            return false;
        }

        boolean cancelSuccessful = false;
        try {
            cancelSuccessful = invokeCancelRequest(mayInterruptIfRunning);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            throw rethrow(e);
        }

        complete(new CancellationException());
        return cancelSuccessful;
    }

    private boolean invokeCancelRequest(boolean mayInterruptIfRunning) throws InterruptedException, ExecutionException {
        waitForRequestToBeSend();

        HazelcastClientInstanceImpl client = (HazelcastClientInstanceImpl) context.getHazelcastInstance();
        if (partitionId > -1) {
            ClientMessage request =
                    ExecutorServiceCancelOnPartitionCodec.encodeRequest(uuid, partitionId, mayInterruptIfRunning);
            ClientInvocation clientInvocation = new ClientInvocation(client, request, objectName, partitionId);
            ClientInvocationFuture f = clientInvocation.invoke();
            return ExecutorServiceCancelOnPartitionCodec.decodeResponse(f.get()).response;
        } else {
            ClientMessage request = ExecutorServiceCancelOnAddressCodec.encodeRequest(uuid, target, mayInterruptIfRunning);
            ClientInvocation clientInvocation = new ClientInvocation(client, request, objectName, target);
            ClientInvocationFuture f = clientInvocation.invoke();
            return ExecutorServiceCancelOnAddressCodec.decodeResponse(f.get()).response;
        }
    }

    private void waitForRequestToBeSend() throws InterruptedException {
        ICompletableFuture future = getFuture();
        ClientInvocationFuture clientCallFuture = (ClientInvocationFuture) future;
        clientCallFuture.getInvocation().getSendConnectionOrWait();
    }

}
