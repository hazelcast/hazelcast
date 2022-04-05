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

package com.hazelcast.internal.util;

import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationResponseHandler;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.spi.properties.ClusterProperty;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import static java.util.logging.Level.FINEST;
import static java.util.logging.Level.WARNING;

/**
 * Class encapsulating local execution with retry logic. The operation must
 * not have an {@link OperationResponseHandler} set and it must return
 * response.
 * The retry will use the configured
 * {@link ClusterProperty#INVOCATION_MAX_RETRY_COUNT} and
 * {@link ClusterProperty#INVOCATION_RETRY_PAUSE}.
 *
 * @see Operation#returnsResponse()
 * @see Operation#getOperationResponseHandler()
 * @see ClusterProperty#INVOCATION_MAX_RETRY_COUNT
 * @see ClusterProperty#INVOCATION_RETRY_PAUSE
 * @see InvocationUtil#executeLocallyWithRetry(NodeEngine, Operation)
 */
public class LocalRetryableExecution implements Runnable, OperationResponseHandler {
    /** Number of times an operation is retried before being logged at WARNING level */
    private static final int LOG_MAX_INVOCATION_COUNT = 99;
    private final ILogger logger;
    private final CountDownLatch done = new CountDownLatch(1);
    private final Operation op;
    private final NodeEngine nodeEngine;
    private final long invocationRetryPauseMillis;
    private final int invocationMaxRetryCount;
    private volatile Object response;
    private int tryCount;

    LocalRetryableExecution(NodeEngine nodeEngine, Operation op) {
        this.nodeEngine = nodeEngine;
        this.logger = nodeEngine.getLogger(LocalRetryableExecution.class);
        this.invocationMaxRetryCount = nodeEngine.getProperties().getInteger(ClusterProperty.INVOCATION_MAX_RETRY_COUNT);
        this.invocationRetryPauseMillis = nodeEngine.getProperties().getMillis(ClusterProperty.INVOCATION_RETRY_PAUSE);
        this.op = op;
        op.setOperationResponseHandler(this);
    }

    /**
     * Causes the current thread to wait until the operation has finished the
     * thread is {@linkplain Thread#interrupt interrupted}, or the specified
     * waiting time elapses. The operation might have finished because it has
     * completed (successfully or with an error) or the retry count has been
     * exceeded.
     *
     * @param timeout the maximum time to wait
     * @param unit    the time unit of the {@code timeout} argument
     * @return {@code true} if the operation completed or the operation retry
     * count has been exceeded, else {@code false}
     * @throws InterruptedException if the current thread is interrupted
     *                              while waiting
     */
    public boolean awaitCompletion(long timeout, TimeUnit unit) throws InterruptedException {
        return done.await(timeout, unit);
    }

    /**
     * The response of the operation execution. It may be an exception if the
     * exception was not an instance of {@link RetryableHazelcastException} or
     * the maximum number of retry counts was exceeded.
     * The response may be also {@code null} if the operation has no response
     * or the operation has not completed yet.
     *
     * @return the operation response
     */
    public Object getResponse() {
        return response;
    }

    @Override
    public void run() {
        nodeEngine.getOperationService().execute(op);
    }

    @Override
    public void sendResponse(Operation op, Object response) {
        tryCount++;
        if (response instanceof RetryableHazelcastException && tryCount < invocationMaxRetryCount) {
            Level level = tryCount > LOG_MAX_INVOCATION_COUNT ? WARNING : FINEST;
            if (logger.isLoggable(level)) {
                logger.log(level, "Retrying local execution: " + toString() + ", Reason: " + response);
            }
            nodeEngine.getExecutionService().schedule(this, invocationRetryPauseMillis, TimeUnit.MILLISECONDS);
        } else {
            this.response = response;
            done.countDown();
        }
    }
}
