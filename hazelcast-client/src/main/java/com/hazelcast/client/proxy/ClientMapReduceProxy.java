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
import com.hazelcast.client.spi.impl.ClientCallFuture;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.logging.Logger;
import com.hazelcast.mapreduce.Collator;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobCompletableFuture;
import com.hazelcast.mapreduce.JobProcessInformation;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import com.hazelcast.mapreduce.TrackableJob;
import com.hazelcast.mapreduce.impl.AbstractJob;
import com.hazelcast.mapreduce.impl.client.ClientCancellationRequest;
import com.hazelcast.mapreduce.impl.client.ClientJobProcessInformationRequest;
import com.hazelcast.mapreduce.impl.client.ClientMapReduceRequest;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.impl.AbstractCompletableFuture;
import com.hazelcast.util.EmptyStatement;
import com.hazelcast.util.UuidUtil;
import com.hazelcast.util.ValidationUtil;

import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ClientMapReduceProxy
        extends ClientProxy
        implements JobTracker {

    private final ConcurrentMap<String, ClientTrackableJob> trackableJobs = new ConcurrentHashMap<String, ClientTrackableJob>();

    public ClientMapReduceProxy(String instanceName, String serviceName, String objectName) {
        super(instanceName, serviceName, objectName);
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
    public <V> TrackableJob<V> getTrackableJob(String jobId) {
        return trackableJobs.get(jobId);
    }

    @Override
    public String toString() {
        return "JobTracker{" + "name='" + getName() + '\'' + '}';
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
        protected <T> JobCompletableFuture<T> invoke(final Collator collator) {
            try {
                final String jobId = UuidUtil.buildRandomUuidString();

                ClientContext context = getContext();
                ClientInvocationService cis = context.getInvocationService();
                ClientMapReduceRequest request = new ClientMapReduceRequest(name, jobId, keys,
                        predicate, mapper, combinerFactory, reducerFactory, keyValueSource,
                        chunkSize, topologyChangedStrategy);

                final ClientCompletableFuture completableFuture = new ClientCompletableFuture(jobId);
                ClientCallFuture future = (ClientCallFuture) cis.invokeOnRandomTarget(request, null);
                future.andThen(new ExecutionCallback() {
                    @Override
                    public void onResponse(Object res) {
                        Object response = res;
                        try {
                            if (collator != null) {
                                response = collator.collate(((Map) response).entrySet());
                            }
                        } finally {
                            completableFuture.setResult(response);
                            trackableJobs.remove(jobId);
                        }
                    }

                    @Override
                    public void onFailure(Throwable throwable) {
                        Throwable t = throwable;
                        try {
                            if (t instanceof ExecutionException
                                    && t.getCause() instanceof CancellationException) {
                                t = t.getCause();
                            }
                            completableFuture.setResult(t);
                        } finally {
                            trackableJobs.remove(jobId);
                        }
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
            extends AbstractCompletableFuture<V>
            implements JobCompletableFuture<V> {

        private final String jobId;
        private final CountDownLatch latch;

        private volatile boolean cancelled;

        protected ClientCompletableFuture(String jobId) {
            super(null, Logger.getLogger(ClientCompletableFuture.class));
            this.jobId = jobId;
            this.latch = new CountDownLatch(1);
        }

        @Override
        public String getJobId() {
            return jobId;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            try {
                cancelled = (Boolean) invoke(new ClientCancellationRequest(getName(), jobId), jobId);
            } catch (Exception ignore) {
                EmptyStatement.ignore(ignore);
            }
            return cancelled;
        }

        @Override
        public boolean isCancelled() {
            return cancelled;
        }

        @Override
        public void setResult(Object result) {
            super.setResult(result);
            latch.countDown();
        }

        @Override
        public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            ValidationUtil.isNotNull(unit, "unit");
            if (!latch.await(timeout, unit) || !isDone()) {
                throw new TimeoutException("timeout reached");
            }
            return getResult();
        }

        @Override
        protected ExecutorService getAsyncExecutor() {
            return getContext().getExecutionService().getAsyncExecutor();
        }
    }

    private final class ClientTrackableJob<V>
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
            return ClientMapReduceProxy.this.getName();
        }

        @Override
        public String getJobId() {
            return jobId;
        }

        @Override
        public ICompletableFuture<V> getCompletableFuture() {
            return completableFuture;
        }

        @Override
        public JobProcessInformation getJobProcessInformation() {
            try {
                return invoke(new ClientJobProcessInformationRequest(getName(), jobId), jobId);
            } catch (Exception ignore) {
                EmptyStatement.ignore(ignore);
            }
            return null;
        }

    }

}
