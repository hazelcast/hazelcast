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

import com.hazelcast.client.impl.ClientMessageDecoder;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ExecutorServiceCancelOnPartitionCodec;
import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.client.spi.impl.ClientInvocationFuture;

import java.util.concurrent.CancellationException;

import static com.hazelcast.util.ExceptionUtil.rethrow;

public class ClientPartitionCancellableDelegatingFuture<T> extends ClientCancellableDelegatingFuture<T> {

    private final int partitionId;

    public ClientPartitionCancellableDelegatingFuture(ClientInvocationFuture future,
                                                      ClientContext context, String uuid,
                                                      int partitionId, T defaultValue,
                                                      ClientMessageDecoder resultDecoder) {
        super(future, context, uuid, defaultValue, resultDecoder);
        this.partitionId = partitionId;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        if (isDone() || cancelled) {
            return false;
        }

        waitForRequestToBeSend();
        ClientInvocationFuture f = invokeCancelRequest(mayInterruptIfRunning);
        try {
            boolean cancelSuccessful = ExecutorServiceCancelOnPartitionCodec.decodeResponse(f.get()).response;
            if (cancelSuccessful) {
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

    private ClientInvocationFuture invokeCancelRequest(boolean mayInterruptIfRunning) {
        ClientInvocation clientInvocation;
        final HazelcastClientInstanceImpl client = (HazelcastClientInstanceImpl) context.getHazelcastInstance();
        ClientMessage request =
                ExecutorServiceCancelOnPartitionCodec.encodeRequest(uuid, partitionId, mayInterruptIfRunning);
        clientInvocation = new ClientInvocation(client, request, partitionId);

        try {
            return clientInvocation.invoke();
        } catch (Exception e) {
            throw rethrow(e);
        }
    }
}
