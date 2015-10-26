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
import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.client.spi.impl.ClientInvocationFuture;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.util.EmptyStatement;

import java.util.concurrent.TimeUnit;

/**
 * An Abstract DelegatingFuture that can cancel a Runnable/Callable that is executed by an
 * {@link com.hazelcast.core.IExecutorService}.
 * It does this by sending a Cancellation Request to the remote owning member and then cancelling the running task.
 *
 * @param <V> Type of returned object from the get method of this class.
 */
public abstract class ClientCancellableDelegatingFuture<V> extends ClientDelegatingFuture<V> {

    private static final int INVOCATION_WAIT_TIMEOUT_SECONDS = 5;
    protected final ClientContext context;
    protected final String uuid;
    protected volatile boolean cancelled;
    private final ILogger logger = Logger.getLogger(ClientCancellableDelegatingFuture.class);

    public ClientCancellableDelegatingFuture(ClientInvocationFuture future, ClientContext context,
                                             String uuid, V defaultValue,
                                             ClientMessageDecoder resultDecoder) {
        super(future, context.getSerializationService(), resultDecoder, defaultValue);
        this.context = context;
        this.uuid = uuid;

    }

    public abstract boolean cancel(boolean mayInterruptIfRunning);

    protected void waitForRequestToBeSend() {
        ClientInvocationFuture clientCallFuture = getFuture();
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
