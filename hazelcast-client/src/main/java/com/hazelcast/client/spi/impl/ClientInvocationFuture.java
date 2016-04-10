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

package com.hazelcast.client.spi.impl;

import com.hazelcast.client.HazelcastClientNotActiveException;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.logging.ILogger;
import com.hazelcast.util.ExceptionUtil;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static com.hazelcast.util.ExceptionUtil.fixAsyncStackTrace;
import static com.hazelcast.util.Preconditions.isNotNull;
import static java.util.concurrent.atomic.AtomicReferenceFieldUpdater.newUpdater;
import static java.util.concurrent.locks.LockSupport.parkNanos;
import static java.util.concurrent.locks.LockSupport.unpark;

public class ClientInvocationFuture implements ICompletableFuture<ClientMessage> {

    private static final AtomicReferenceFieldUpdater<ClientInvocationFuture, Object> STATE =
            newUpdater(ClientInvocationFuture.class, Object.class, "state");

    private static final Object VOID = new Object() {
        public String toString() {
            return "VOID";
        }
    };

    protected final ILogger logger;

    protected final ClientMessage clientMessage;
    protected volatile Object state = VOID;

    private final ClientExecutionServiceImpl executionService;
    private final ClientInvocation invocation;

    public ClientInvocationFuture(ClientInvocation invocation, HazelcastClientInstanceImpl client,
                                  ClientMessage clientMessage, ILogger logger) {

        this.executionService = (ClientExecutionServiceImpl) client.getClientExecutionService();
        this.clientMessage = clientMessage;
        this.invocation = invocation;
        this.logger = logger;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public boolean isDone() {
        return isDone(state);
    }

    private boolean isDone(final Object state) {
        if (state == null) {
            return true;
        }

        return !(state == VOID
                || state.getClass() == WaitNode.class
                || state instanceof Thread
                || state instanceof ExecutionCallback);
    }

    @Override
    public ClientMessage get() throws InterruptedException, ExecutionException {
        try {
            return get(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        } catch (TimeoutException exception) {
            throw ExceptionUtil.rethrow(exception);
        }
    }

    @Override
    public ClientMessage get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        long timeoutNanos = unit.toNanos(timeout);

        if (timeoutNanos <= 0) {
            Object response = state;
            if (isDone(response)) {
                return resolveAndThrow(response);
            } else {
                return resolveAndThrow(null);
            }
        }

        Object response = registerWaiter(Thread.currentThread(), null);
        if (response != VOID) {
            return resolveAndThrow(response);
        }

        long startTimeNanos = System.nanoTime();
        // todo: deal with interruption
        // we are going to park for a result; however it can be that we get spurious wake-ups so
        // we need to recheck the state. We don't need to re-registerWaiter
        for (; ; ) {
            parkNanos(timeoutNanos);
            long endTimeNanos = System.nanoTime();
            timeoutNanos -= endTimeNanos - startTimeNanos;
            startTimeNanos = endTimeNanos;

            Object state = this.state;
            if (isDone(state)) {
                return resolveAndThrow(state);
            } else if (timeoutNanos <= 0) {
                return resolveAndThrow(null);
            }
        }
//
//
//        if (response == null) {
//            long waitMillis = unit.toMillis(timeout);
//            if (waitMillis > 0) {
//                synchronized (this) {
//                    while (waitMillis > 0 && response == null) {
//                        long start = Clock.currentTimeMillis();
//                        wait(waitMillis);
//                        long elapsed = Clock.currentTimeMillis() - start;
//                        waitMillis -= elapsed;
//                    }
//                }
//            }
//        }
//        return resolveAndThrow();
    }

//    /**
//     * @param response coming from server
//     * @return true if response coming from server should be set
//     */
//    boolean shouldSetResponse(Object response) {
//        if (this.response != null) {
//            logger.warning("The Future.set() method can only be called once. Request: " + clientMessage
//                    + ", current response: " + this.response + ", new response: " + response);
//            return false;
//        }
//        return true;
//    }
//

//    void complete(Object response) {
//        synchronized (this) {
//            if (!shouldSetResponse(response)) {
//                return;
//            }
//
//            this.response = response;
//            notifyAll();
//            for (ExecutionCallbackNode node : callbackNodeList) {
//                runAsynchronous(node.callback, node.executor);
//            }
//            callbackNodeList.clear();
//        }
//    }

    public boolean complete(Object value) {
        for (; ; ) {
            Object oldState = state;
            if (isDone(oldState)) {
                return false;
            }

            if (STATE.compareAndSet(this, oldState, value)) {
                unblockAll(oldState, null);
                return true;
            }
        }
    }

    private ClientMessage resolveAndThrow(Object response) throws ExecutionException, TimeoutException, InterruptedException {
        if (response instanceof Throwable) {
            fixAsyncStackTrace((Throwable) response, Thread.currentThread().getStackTrace());
            if (response instanceof ExecutionException) {
                throw (ExecutionException) response;
            }
            if (response instanceof TimeoutException) {
                throw (TimeoutException) response;
            }
            if (response instanceof Error) {
                throw (Error) response;
            }
            if (response instanceof InterruptedException) {
                throw (InterruptedException) response;
            }
            throw new ExecutionException((Throwable) response);
        }
        if (response == null) {
            throw new TimeoutException();
        }
        return (ClientMessage) response;
    }

    @Override
    public void andThen(ExecutionCallback<ClientMessage> callback) {
        andThen(callback, executionService.getAsyncExecutor());
    }

    @Override
    public void andThen(ExecutionCallback<ClientMessage> callback, Executor executor) {
        isNotNull(callback, "callback");
        isNotNull(executor, "executor");

        Object response = registerWaiter(callback, executor);
        if (response != VOID) {
            unblock(callback, executor);
        }
    }

    private void unblockAll(Object waiter, Executor executor) {
        while (waiter != null) {
            if (waiter instanceof Thread) {
                unpark((Thread) waiter);
                return;
            } else if (waiter instanceof ExecutionCallback) {
                unblock((ExecutionCallback) waiter, executor);
                return;
            } else if (waiter.getClass() == WaitNode.class) {
                WaitNode waitNode = (WaitNode) waiter;
                unblockAll(waitNode.waiter, waitNode.executor);
                waiter = waitNode.next;
            } else {
                return;
            }
        }
    }

    private void unblock(final ExecutionCallback callback, Executor executor) {
        try {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        ClientMessage resp;
                        try {
                            resp = resolveAndThrow(state);
                        } catch (Throwable t) {
                            callback.onFailure(t);
                            return;
                        }
                        callback.onResponse(resp);
                    } catch (Throwable t) {
                        logger.severe("Failed to execute callback: " + callback
                                + "! Request: " + clientMessage + ", response: " + state, t);
                    }
                }
            });
        } catch (RejectedExecutionException e) {
            logger.warning("Execution of callback: " + callback + " is rejected!", e);
            callback.onFailure(new HazelcastClientNotActiveException(e.getMessage()));
        }
    }

    private Object registerWaiter(Object waiter, Executor executor) {
        WaitNode waitNode = null;
        for (; ; ) {
            final Object oldState = state;
            if (isDone(oldState)) {
                return oldState;
            }

            Object newState;
            if (oldState == VOID && (executor == null)) {
                // Nothing is syncing on this future, so instead of creating a WaitNode, we just try
                // the cas the thread (so no extra litter)
                newState = waiter;
            } else {
                // something already has been registered for syncing. So we need to create a WaitNode.
                if (waitNode == null) {
                    waitNode = new WaitNode(waiter, executor);
                }
                waitNode.next = oldState;
                newState = waitNode;
            }

            if (STATE.compareAndSet(this, oldState, newState)) {
                // we have successfully registered to be notified.
                return VOID;
            }
        }
    }

    public ClientInvocation getInvocation() {
        return invocation;
    }

    private static final class WaitNode {
        private final Executor executor;
        private final Object waiter;
        private Object next;

        private WaitNode(Object waiter, Executor executor) {
            this.waiter = waiter;
            this.executor = executor;
        }
    }
}
