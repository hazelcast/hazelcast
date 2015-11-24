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

package com.hazelcast.client.proxy;

import com.hazelcast.client.connection.nio.ClientConnection;
import com.hazelcast.client.impl.client.InvocationClientRequest;
import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.client.spi.impl.ClientInvocationFuture;
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

import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

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
    public <V> TrackableJob<V> getTrackableJob(String jobId) {
        return trackableJobs.get(jobId);
    }

    @Override
    public String toString() {
        return "JobTracker{" + "name='" + getName() + '\'' + '}';
    }

    private <T> T invoke(InvocationClientRequest request, String jobId) throws Exception {
        ClientTrackableJob trackableJob = trackableJobs.get(jobId);
        if (trackableJob != null) {
            ClientConnection sendConnection = trackableJob.clientInvocation.getSendConnection();
            Address runningMember = sendConnection.getEndPoint();
            final ClientInvocation clientInvocation = new ClientInvocation(getClient(), request, runningMember);
            ICompletableFuture<T> future = clientInvocation.invoke();
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
                final String jobId = UuidUtil.newUnsecureUuidString();

                ClientMapReduceRequest request = new ClientMapReduceRequest(name, jobId, keys,
                        predicate, mapper, combinerFactory, reducerFactory, keyValueSource,
                        chunkSize, topologyChangedStrategy);

                final ClientCompletableFuture completableFuture = new ClientCompletableFuture(jobId);

                final ClientInvocation clientInvocation = new ClientInvocation(getClient(), request);
                final ClientInvocationFuture future = clientInvocation.invoke();

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

                trackableJobs.putIfAbsent(jobId, new ClientTrackableJob<T>(jobId, clientInvocation, completableFuture));
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

        protected ClientCompletableFuture(String jobId) {
            super(getContext().getExecutionService().getAsyncExecutor(), Logger.getLogger(ClientCompletableFuture.class));
            this.jobId = jobId;
        }

        @Override
        public String getJobId() {
            return jobId;
        }

        @Override
        protected boolean shouldCancel(boolean mayInterruptIfRunning) {
            boolean cancelled = false;
            try {
                cancelled = (Boolean) invoke(new ClientCancellationRequest(getName(), jobId), jobId);
            } catch (Exception ignore) {
                EmptyStatement.ignore(ignore);
            }
            return cancelled;
        }

        @Override
        protected void setResult(Object result) {
            super.setResult(result);
        }
    }

    private final class ClientTrackableJob<V>
            implements TrackableJob<V> {

        private final String jobId;
        private final ClientInvocation clientInvocation;
        private final AbstractCompletableFuture<V> completableFuture;

        private ClientTrackableJob(String jobId, ClientInvocation clientInvocation,
                                   AbstractCompletableFuture<V> completableFuture) {
            this.jobId = jobId;
            this.clientInvocation = clientInvocation;
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
