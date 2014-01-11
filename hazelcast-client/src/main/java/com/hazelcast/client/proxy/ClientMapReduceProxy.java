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

import com.hazelcast.client.InvocationClientRequest;
import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.client.spi.ClientInvocationService;
import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.client.spi.impl.ClientCallFuture;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.logging.Logger;
import com.hazelcast.mapreduce.Collator;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobProcessInformation;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import com.hazelcast.mapreduce.impl.AbstractJob;
import com.hazelcast.mapreduce.impl.TrackableJob;
import com.hazelcast.mapreduce.impl.client.ClientCancellationRequest;
import com.hazelcast.mapreduce.impl.client.ClientJobProcessInformationRequest;
import com.hazelcast.mapreduce.impl.client.ClientMapReduceRequest;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.impl.AbstractCompletableFuture;
import com.hazelcast.util.Clock;
import com.hazelcast.util.ValidationUtil;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ClientMapReduceProxy
        extends ClientProxy
        implements JobTracker {

    private final ConcurrentMap<String, ClientTrackableJob> trackableJobs = new ConcurrentHashMap<String, ClientTrackableJob>();

    public ClientMapReduceProxy(String serviceName, String objectName) {
        super(serviceName, objectName);
    }

    @Override
    protected void onDestroy() {
        for (ClientTrackableJob trackableJob : trackableJobs.values()) {
            trackableJob.completableFuture.cancel(false);
        }
    }

    @Override
    public <K, V> Job<K, V> newJob(KeyValueSource<K, V> source) {
        return new ClientJob<K, V>(getName(), source);
    }

    @Override
    public JobProcessInformation getJobProcessInformation(String jobId) {
        try {
            return invoke(new ClientJobProcessInformationRequest(getName(), jobId), jobId);
        } catch (Exception ignore) {
        }
        return null;
    }

    /*
     * Removed for now since it is moved to Hazelcast 3.3
    @Override
    public <K, V> ProcessJob<K, V> newProcessJob(KeyValueSource<K, V> source) {
        // TODO
        return null;
    }*/

    private <T> T invoke(InvocationClientRequest request, String jobId) throws Exception {
        ClientContext context = getContext();
        ClientInvocationService cis = context.getInvocationService();
        ClientTrackableJob trackableJob = trackableJobs.get(jobId);
        if (trackableJob != null) {
            Address runningMember = trackableJob.jobOwner;
            ICompletableFuture<T> future = cis.invokeOnTarget(request, runningMember);
            return future.get();
        }
        return null;
    }

    private class ClientJob<KeyIn, ValueIn> extends AbstractJob<KeyIn, ValueIn> {

        public ClientJob(String name, KeyValueSource<KeyIn, ValueIn> keyValueSource) {
            super(name, ClientMapReduceProxy.this, keyValueSource);
        }

        @Override
        protected <T> ICompletableFuture<T> invoke(final Collator collator) {
            try {
                ClientContext context = getContext();
                ClientInvocationService cis = context.getInvocationService();
                ClientMapReduceRequest request = new ClientMapReduceRequest(name, jobId, keys,
                        predicate, mapper, combinerFactory, reducerFactory, keyValueSource, chunkSize);

                final ClientCompletableFuture completableFuture = new ClientCompletableFuture(jobId);
                ClientCallFuture future = (ClientCallFuture) cis.invokeOnRandomTarget(request, new EventHandler() {
                    @Override
                    public void handle(Object event) {
                        if (collator != null) {
                            event = collator.collate(((Map) event).entrySet());
                        }
                        completableFuture.setResult(event);
                    }
                });

                Address runningMember = future.getConnection().getRemoteEndpoint();
                trackableJobs.putIfAbsent(jobId, new ClientTrackableJob<T>(jobId, runningMember, completableFuture));
                return completableFuture;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

    }

    private class ClientCompletableFuture<V>
            extends AbstractCompletableFuture<V> {

        private final String jobId;

        private volatile boolean cancelled;

        protected ClientCompletableFuture(String jobId) {
            super(null, Logger.getLogger(ClientCompletableFuture.class));
            this.jobId = jobId;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            try {
                cancelled = invoke(new ClientCancellationRequest(getName(), jobId), jobId);
            } catch (Exception ignore) {
            }
            return cancelled;
        }

        @Override
        public boolean isCancelled() {
            return cancelled;
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
            return getResult();
        }

        @Override
        protected ExecutorService getAsyncExecutor() {
            return getContext().getExecutionService().getAsyncExecutor();
        }
    }

    private class ClientTrackableJob<V>
            implements TrackableJob<V> {

        private final String jobId;
        private final Address jobOwner;
        private final AbstractCompletableFuture<V> completableFuture;

        private ClientTrackableJob(String jobId, Address jobOwner,
                                   AbstractCompletableFuture<V> completableFuture) {
            this.jobId = jobId;
            this.jobOwner = jobOwner;
            this.completableFuture = completableFuture;
        }

        @Override
        public JobTracker getJobTracker() {
            return ClientMapReduceProxy.this;
        }

        @Override
        public String getName() {
            return getName();
        }

        @Override
        public String getJobId() {
            return jobId;
        }

        @Override
        public ICompletableFuture<V> getCompletableFuture() {
            return completableFuture;
        }
    }

}
