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

package com.hazelcast.client.proxy;

import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.client.spi.ClientInvocationService;
import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.client.spi.ResponseHandler;
import com.hazelcast.client.spi.ResponseStream;
import com.hazelcast.client.util.ErrorHandler;
import com.hazelcast.core.CompletableFuture;
import com.hazelcast.mapreduce.Collator;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import com.hazelcast.mapreduce.impl.AbstractJob;
import com.hazelcast.mapreduce.impl.client.ClientMapReduceRequest;
import com.hazelcast.mapreduce.process.ProcessJob;
import com.hazelcast.spi.impl.AbstractCompletableFuture;
import com.hazelcast.util.Clock;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.ValidationUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ClientMapReduceProxy extends ClientProxy implements JobTracker {

    public ClientMapReduceProxy(String serviceName, String objectName) {
        super(serviceName, objectName);
    }

    @Override
    protected void onDestroy() {

    }

    @Override
    public <K, V> Job<K, V> newJob(KeyValueSource<K, V> source) {
        return new ClientJob<K, V>(getName(), source);
    }

    @Override
    public <K, V> ProcessJob<K, V> newProcessJob(KeyValueSource<K, V> source) {
        // TODO
        return null;
    }

    private class ClientJob<KeyIn, ValueIn> extends AbstractJob<KeyIn, ValueIn> {

        public ClientJob(String name, KeyValueSource<KeyIn, ValueIn> keyValueSource) {
            super(name, ClientMapReduceProxy.this, keyValueSource);
        }

        @Override
        protected <T> CompletableFuture<T> invoke(final Collator collator) {
            try {
                ClientContext context = getContext();
                ClientInvocationService cis = context.getInvocationService();
                ClientMapReduceRequest request = new ClientMapReduceRequest(name, jobId, new ArrayList(keys),
                        predicate, mapper, combinerFactory, reducerFactory, keyValueSource, chunkSize);

                final ClientCompletableFuture completableFuture = new ClientCompletableFuture();
                cis.invokeOnRandomTarget(request, new ResponseHandler() {
                    @Override
                    public void handle(ResponseStream stream) throws Exception {
                        try {
                            stream.read(); // initial ok response
                            Object event = stream.read();
                            if (collator != null) {
                                event = collator.collate(((Map) event).entrySet());
                            }
                            completableFuture.setResult(event);
                        } catch (Exception e) {
                            try {
                                stream.end();
                            } catch (IOException ignored) {
                            }
                            if (ErrorHandler.isRetryable(e)) {
                                throw e;
                            }
                        }
                    }
                });

                return completableFuture;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

    }

    private class ClientCompletableFuture<V> extends AbstractCompletableFuture<V> {

        protected ClientCompletableFuture() {
            super(null);
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
        public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            ValidationUtil.isNotNull(unit, "unit");
            long deadline = timeout == 0L ? -1 : Clock.currentTimeMillis() + unit.toMillis(timeout);
            for (; ; ) {
                try {
                    Thread.sleep(100);
                } catch (Exception e) {
                    if (e instanceof InterruptedException) {
                        throw (InterruptedException) e;
                    }
                }

                if (isDone()) {
                    break;
                }

                long delta = deadline - Clock.currentTimeMillis();
                if (delta <= 0L) {
                    throw new TimeoutException("timeout reached");
                }
            }
            return (V) getResult();
        }

        @Override
        protected ExecutorService getAsyncExecutor() {
            return getContext().getExecutionService().getAsyncExecutor();
        }
    }

}
