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

package com.hazelcast.jet.impl;

import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.JetCancelJobCodec;
import com.hazelcast.client.impl.protocol.codec.JetCompleteResourceCodec;
import com.hazelcast.client.impl.protocol.codec.JetExecuteJobCodec;
import com.hazelcast.client.impl.protocol.codec.JetUpdateResourceCodec;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.client.spi.impl.ClientInvocationFuture;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.jet.DAG;
import com.hazelcast.jet.JetConfig;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.JobConfig;
import com.hazelcast.jet.ResourceConfig;
import com.hazelcast.jet.impl.deployment.ResourceIterator;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.util.function.Supplier;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.util.stream.Collectors.toList;

public class JetClientInstanceImpl extends AbstractJetInstance {

    private final HazelcastClientInstanceImpl client;
    private final ILogger logger;

    public JetClientInstanceImpl(HazelcastClientInstanceImpl hazelcastInstance) {
        super(hazelcastInstance);
        this.client = hazelcastInstance;
        this.logger = hazelcastInstance.getLoggingService().getLogger(JetInstance.class);
    }

    @Override
    public JetConfig getConfig() {
        throw new UnsupportedOperationException("Jet Configuration is not available on the client");
    }

    @Override
    public Job newJob(DAG dag) {
        return new JobImpl(dag);
    }

    @Override
    public Job newJob(DAG dag, JobConfig config) {
        return new JobImpl(dag, config);
    }

    private class JobImpl implements Job {

        private final DAG dag;
        private final JobConfig config;

        protected JobImpl(DAG dag) {
            this(dag, new JobConfig());
        }

        protected JobImpl(DAG dag, JobConfig config) {
            this.dag = dag;
            this.config = config;
        }

        @Override
        public Future<Void> execute() {
            long executionId = getIdGenerator().newId();
            deployResources(executionId);
            Data dagData = client.getSerializationService().toData(dag);
            int partitionId = client.getPartitionService().getPartition(executionId).getPartitionId();
            ClientInvocation invocation = new ClientInvocation(client,
                    JetExecuteJobCodec.encodeRequest(executionId, dagData), partitionId);
            return new ExecutionFuture(invocation.invoke(), executionId, partitionId);
        }

        private void deployResources(long executionId) {
            final Set<ResourceConfig> resources = config.getResourceConfigs();
            new ResourceIterator(resources, config.getResourcePartSize()).forEachRemaining(part -> {
                Data partData = client.getSerializationService().toData(part);
                invokeOnCluster(() -> JetUpdateResourceCodec.encodeRequest(executionId, partData));
            });
            resources.forEach(r -> {
                Data descriptorData = client.getSerializationService().toData(r.getDescriptor());
                invokeOnCluster(() -> JetCompleteResourceCodec.encodeRequest(executionId, descriptorData));
            });
        }

        private List<ClientMessage> invokeOnCluster(Supplier<ClientMessage> messageSupplier) {
            return client.getCluster().getMembers().stream()
                         .map(m -> new ClientInvocation(client, messageSupplier.get(), m.getAddress()).invoke())
                         .collect(toList())
                         .stream()
                         .map(Util::uncheckedGet)
                         .collect(toList());
        }


    }

    private final class ExecutionFuture implements Future<Void> {

        private final ClientInvocationFuture future;
        private final long executionId;
        private final int partitionId;

        protected ExecutionFuture(ClientInvocationFuture future, long executionId, int partitionId) {
            this.future = future;
            this.executionId = executionId;
            this.partitionId = partitionId;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            boolean cancelled = future.cancel(true);
            if (!cancelled) {
                return false;
            }
            new ClientInvocation(client, JetCancelJobCodec.encodeRequest(executionId), partitionId)
                    .invoke().andThen(new ExecutionCallback<ClientMessage>() {
                @Override
                public void onResponse(ClientMessage clientMessage) {
                    //ignored
                }

                @Override
                public void onFailure(Throwable throwable) {
                    logger.warning("Error cancelling job with id " + executionId, throwable);
                }
            });
            return true;
        }

        @Override
        public boolean isCancelled() {
            return future.isCancelled();
        }

        @Override
        public boolean isDone() {
            return future.isDone();
        }

        @Override
        public Void get() throws InterruptedException, ExecutionException {
            future.get();
            return null;
        }

        @Override
        public Void get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            future.get(timeout, unit);
            return null;
        }
    }
}
