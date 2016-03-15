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

package com.hazelcast.internal.util;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.logging.ILogger;
import com.hazelcast.util.ExceptionUtil;

import java.util.concurrent.Executor;

public abstract class VeryAbstractCompletableFuture<V> implements ICompletableFuture<V> {

    protected final ILogger logger;
    protected final Executor defaultExecutor;

    public VeryAbstractCompletableFuture(Executor defaultExecutor, ILogger logger) {
        this.logger = logger;
        this.defaultExecutor = defaultExecutor;
    }

    @Override
    public void andThen(ExecutionCallback<V> callback) {
        andThen(callback, defaultExecutor);
    }

    public V join() {
        try {
            //this method is quite inefficient when there is unchecked exception, because it will be wrapped
            //in a ExecutionException, and then it is unwrapped again.
            return get();
        } catch (Throwable throwable) {
            throw ExceptionUtil.rethrow(throwable);
        }
    }

    public V getSafely() {
        return join();
    }

    protected void runAsynchronous(ExecutionCallbackNode head, Object result) {
        while (head != null) {
            runAsynchronous(head.callback, head.executor, result);
            head = head.next;
        }
    }

    protected void runAsynchronous(ExecutionCallback<V> callback, Executor executor, Object result) {
        if (callback != null) {
            executor.execute(new ExecutionCallbackRunnable<V>(getClass(), result, callback));
        }
    }

    protected Object resolve(Object result) {
        return result;
    }

    protected static final class ExecutionCallbackNode<E> {
        protected final ExecutionCallback<E> callback;
        protected final Executor executor;
        protected final ExecutionCallbackNode<E> next;

        public ExecutionCallbackNode(ExecutionCallback<E> callback, Executor executor, ExecutionCallbackNode<E> next) {
            this.callback = callback;
            this.executor = executor;
            this.next = next;
        }
    }

    protected final class ExecutionCallbackRunnable<V> implements Runnable {
        private final Class<?> caller;
        private final Object result;
        private final ExecutionCallback<V> callback;

        protected ExecutionCallbackRunnable(Class<?> caller, Object result, ExecutionCallback<V> callback) {
            this.caller = caller;
            this.result = result;
            this.callback = callback;
        }

        @Override
        public void run() {
            try {
                Object response = resolve(result);

                if (response instanceof Throwable) {
                    callback.onFailure((Throwable) response);
                } else {
                    callback.onResponse((V) response);
                }
            } catch (Throwable cause) {
                logger.severe("Failed asynchronous execution of execution callback: " + callback + " for call " + caller, cause);
            }
        }
    }
}
