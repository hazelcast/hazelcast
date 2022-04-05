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

package com.hazelcast.client.impl.protocol.task;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static com.hazelcast.internal.util.ConcurrencyUtil.CALLER_RUNS;

/**
 *
 * Base message task for async tasks
 *
 * @param <P> The client message parameter class
 * @param <T> The task return type
 */
public abstract class AbstractAsyncMessageTask<P, T> extends AbstractMessageTask<P> {

    protected AbstractAsyncMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    /**
     * Called on node side, before starting any operation.
     */
    protected void beforeProcess() {
    }

    /**
     * Called on node side, after process is run and right before sending the response to the client.
     *
     * This method gives the task a chance to change the response object to be sent to the client
     *
     * @param response The response for the task. It can be modified to a different object by overriding this method.
     * @return The object to be sent to the client. The user may choose to return a different
     */
    protected Object processResponseBeforeSending(T response) {
        return response;
    }

    /**
     * Called on node side, after sending the response to the client.
     *
     * @param response  The response sent to the client
     * @param throwable The throwable to be sent to the client if an exception occurred
     */
    protected void afterSendingResponse(Object response, Throwable throwable) {
    }

    @Override
    protected void processMessage() {
        beforeProcess();
        CompletableFuture<T> internalFuture = processInternal();
        internalFuture.thenApplyAsync(this::processResponseBeforeSending, CALLER_RUNS)
                      .whenCompleteAsync(this::sendResponseOrHandleFailure, CALLER_RUNS)
                      .whenCompleteAsync(this::afterSendingResponse, CALLER_RUNS);
    }

    private void sendResponseOrHandleFailure(Object response, Throwable throwable) {
        if (throwable == null) {
            sendResponse(response);
        } else {
            if (throwable instanceof CompletionException) {
                throwable = throwable.getCause();
            }
            handleProcessingFailure(throwable);
        }
    }

    protected abstract CompletableFuture<T> processInternal();
}
