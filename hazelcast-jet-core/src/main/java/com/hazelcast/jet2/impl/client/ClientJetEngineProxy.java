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

package com.hazelcast.jet2.impl.client;

import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.JetCompleteResourceCodec;
import com.hazelcast.client.impl.protocol.codec.JetCreateEngineIfAbsentCodec;
import com.hazelcast.client.impl.protocol.codec.JetExecuteJobCodec;
import com.hazelcast.client.impl.protocol.codec.JetUpdateResourceCodec;
import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.client.spi.impl.ClientInvocationFuture;
import com.hazelcast.core.IdGenerator;
import com.hazelcast.jet2.DAG;
import com.hazelcast.jet2.JetEngine;
import com.hazelcast.jet2.JetEngineConfig;
import com.hazelcast.jet2.Job;
import com.hazelcast.jet2.ResourceConfig;
import com.hazelcast.jet2.impl.JetEngineProxy;
import com.hazelcast.jet2.impl.JetService;
import com.hazelcast.jet2.impl.JobImpl;
import com.hazelcast.jet2.impl.Util;
import com.hazelcast.jet2.impl.deployment.ResourceIterator;
import com.hazelcast.nio.serialization.Data;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.hazelcast.jet2.impl.JetEngineProxyImpl.ID_GENERATOR_PREFIX;

public class ClientJetEngineProxy extends ClientProxy implements JetEngineProxy {
    private IdGenerator idGenerator;

    public ClientJetEngineProxy(String serviceName, String name) {
        super(serviceName, name);
    }

    @Override
    protected void onInitialize() {
        super.onInitialize();
        idGenerator = getClient().getIdGenerator(ID_GENERATOR_PREFIX + name);
    }

    @Override
    public Job newJob(DAG dag) {
        return new JobImpl(this, dag);
    }

    @Override
    public Future<Void> execute(JobImpl job) {
        Data dag = toData(job.getDag());
        ClientInvocation invocation =
                new ClientInvocation(getClient(), JetExecuteJobCodec.encodeRequest(name, idGenerator.newId(), dag));
        return new JobExecutionFuture(invocation.invoke());
    }

    private void deployResources(JetEngineConfig config) {
        final Set<ResourceConfig> resources = config.getResourceConfigs();
        new ResourceIterator(resources, config.getResourcePartSize()).forEachRemaining(part -> {
            Data partData = toData(part);
            invokeOnCluster(getClient(), () -> JetUpdateResourceCodec.encodeRequest(name, partData));
        });
        resources.forEach(r -> {
            Data descriptorData = toData(r.getDescriptor());
            invokeOnCluster(getClient(), () -> JetCompleteResourceCodec.encodeRequest(name, descriptorData));
        });
    }

    public static JetEngine createEngine(String name, JetEngineConfig config, HazelcastClientInstanceImpl client) {
        final Data data = client.getSerializationService().toData(config);
        boolean engineCreated =
                invokeOnCluster(client, () -> JetCreateEngineIfAbsentCodec.encodeRequest(name, data))
                        .stream()
                        .map(m -> JetCreateEngineIfAbsentCodec.decodeResponse(m).response)
                        .anyMatch(p -> p);
        ClientJetEngineProxy proxy = client.getDistributedObject(JetService.SERVICE_NAME, name);

        if (engineCreated) {
            proxy.deployResources(config);
        }
        return proxy;
    }

    private static List<ClientMessage> invokeOnCluster(HazelcastClientInstanceImpl client,
                                                       Supplier<ClientMessage> messageSupplier) {
        return client.getCluster().getMembers().stream()
                     .map(m -> new ClientInvocation(client, messageSupplier.get(), m.getAddress()).invoke())
                     .map(Util::uncheckedGet).collect(Collectors.toList());
    }

    private static final class JobExecutionFuture implements Future<Void> {

        private final ClientInvocationFuture future;

        private JobExecutionFuture(ClientInvocationFuture future) {
            this.future = future;
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

