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

package com.hazelcast.spi.impl;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.AsyncInvocationService;
import com.hazelcast.spi.Invocation;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.ResponseQueueFactory;

import java.util.concurrent.*;
import java.util.logging.Level;

/**
 * @mdogan 1/21/13
 */
public class AsyncInvocationServiceImpl implements AsyncInvocationService {

    private static final int TIMEOUT = 10;

    private final NodeEngine nodeEngine;
    private final Executor executor;
    private final ILogger logger;

    public AsyncInvocationServiceImpl(NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
        String executorName = "hz:async-service";
        final ExecutionServiceImpl executionService = (ExecutionServiceImpl) nodeEngine.getExecutionService();
        this.executor = executionService.register(executorName, 20, 10000);  // TODO: @mm - both sizes should be configurable.
        logger = nodeEngine.getLogger(AsyncInvocationService.class.getName());
    }

    public Future invoke(final Invocation invocation) {
        final FutureProxy futureProxy = new FutureProxy();
        invoke((InvocationImpl) invocation, futureProxy);
        return futureProxy;
    }

    public void invoke(Invocation invocation, ExecutionCallback callback) {
        invoke((InvocationImpl) invocation, new ExecutionCallbackAdapter(callback));
    }

    private void invoke(final InvocationImpl invocation, final Callback<Object> responseCallback) {
        try {
            executor.execute(new AsyncInvocation(invocation, responseCallback));
        } catch (RejectedExecutionException e) {
            logger.log(Level.WARNING, "Capacity overloaded! Executing " + invocation.getOperation()
                    + " in current thread, instead of invoking async.");
            invokeInCurrentThread(invocation, responseCallback);
        }
    }

    private void invokeInCurrentThread(InvocationImpl invocation, Callback<Object> responseCallback) {
        invocation.invoke();
        waitAndSetResult(invocation, responseCallback, Long.MAX_VALUE);
    }

    private boolean waitAndSetResult(InvocationImpl invocation, Callback<Object> responseCallback, long timeout) {
        Object result = null;
        try {
            result = invocation.doGet(timeout, TimeUnit.MILLISECONDS);
        } catch (Throwable e) {
            result = e;
        }
        if (result != InvocationImpl.TIMEOUT_RESPONSE) { // if timeout is smaller than long.max
            responseCallback.notify(result);
            return true;
        }
        return false;
    }

    private class AsyncInvocation implements Runnable {
        private final InvocationImpl invocation;
        private final Callback<Object> responseCallback;

        private AsyncInvocation(InvocationImpl invocation, Callback<Object> responseCallback) {
            this.invocation = invocation;
            this.responseCallback = responseCallback;
        }

        public void run() {
            invocation.setCallback(new AsyncInvocationCallback(responseCallback));
            invocation.invoke();
        }
    }

    private class AsyncInvocationCallback implements Callback<InvocationImpl> {

        private final Callback<Object> responseCallback;

        private AsyncInvocationCallback(Callback<Object> responseCallback) {
            this.responseCallback = responseCallback;
        }

        public void notify(InvocationImpl invocation) {
            if (responseCallback instanceof FutureProxy) {
                waitAndSetResult(invocation, responseCallback, TIMEOUT);
            } else {
                executor.execute(new AsyncInvocationNotifier(invocation, responseCallback));
            }
        }
    }

    private class AsyncInvocationNotifier implements Runnable {
        private final InvocationImpl invocation;
        private final Callback<Object> responseCallback;

        private AsyncInvocationNotifier(InvocationImpl invocation, Callback<Object> responseCallback) {
            this.invocation = invocation;
            this.responseCallback = responseCallback;
        }

        public void run() {
            waitAndSetResult(invocation, responseCallback, TIMEOUT);
        }
    }

    private class ExecutionCallbackAdapter implements Callback<Object> {

        private final ExecutionCallback executionCallback;

        private ExecutionCallbackAdapter(ExecutionCallback executionCallback) {
            this.executionCallback = executionCallback;
        }

        public void notify(Object response) {
            try {
                if (response instanceof Throwable) {
                    executionCallback.onFailure((Throwable) response);
                } else {
                    executionCallback.onResponse(InvocationImpl.NULL_RESPONSE != response ? response : null);
                }
            } catch (Throwable e) {
                logger.log(Level.WARNING, e.getMessage(), e);
            }
        }
    }

    private class FutureProxy implements Future, Callback<Object> {

        private final BlockingQueue<Object> ref = ResponseQueueFactory.newResponseQueue();
        private volatile boolean done = false;

        public Object get() throws InterruptedException, ExecutionException {
            final Object result = ref.take();
            try {
                return InvocationImpl.resolveResponse(result);
            } catch (TimeoutException e) {
                throw new HazelcastException("Never happens!", e);
            }
        }

        public Object get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            final Object result = ref.poll(timeout, unit);
            if (result == null) {
                throw new TimeoutException();
            }
            return InvocationImpl.resolveResponse(result);
        }

        public void notify(Object response) {
            ref.offer(response);
        }

        public boolean cancel(boolean mayInterruptIfRunning) {
            done = true;
            return false;
        }

        public boolean isCancelled() {
            return false;
        }

        public boolean isDone() {
            return done;
        }
    }

    void shutdown() {
    }

}
