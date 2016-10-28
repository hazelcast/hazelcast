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

package com.hazelcast.jet2.impl;

import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.Member;
import com.hazelcast.jet.impl.util.JetUtil;
import com.hazelcast.jet2.DAG;
import com.hazelcast.jet2.DeploymentConfig;
import com.hazelcast.jet2.Edge;
import com.hazelcast.jet2.JetEngine;
import com.hazelcast.jet2.Job;
import com.hazelcast.jet2.Vertex;
import com.hazelcast.jet2.impl.deployment.ChunkIterator;
import com.hazelcast.jet2.impl.deployment.DeployChunkOperation;
import com.hazelcast.jet2.impl.deployment.UpdateDeploymentCatalogOperation;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Supplier;

import static com.hazelcast.jet.impl.util.JetUtil.unchecked;
import static java.util.stream.Collectors.toList;

public class JetEngineImpl extends AbstractDistributedObject<JetService> implements JetEngine {

    @SuppressWarnings("checkstyle:magicnumber")
    private static final int DEFAULT_RESOURCE_CHUNK_SIZE = 1 << 14;
    private final String name;
    private final ILogger logger;
    private final ExecutionContext executionContext;

    JetEngineImpl(String name, NodeEngine nodeEngine, JetService service) {
        super(nodeEngine, service);
        this.name = name;
        this.logger = nodeEngine.getLogger(JetEngine.class);
        executionContext = service.getExecutionContext(name);
    }

    public void initializeDeployment() {
        invokeDeployment(executionContext.getConfig().getDeploymentConfigs());
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getServiceName() {
        return JetService.SERVICE_NAME;
    }

    @Override
    public Job newJob(DAG dag) {
        return new JobImpl(this, dag);
    }

    public void execute(JobImpl job) {
        try {
            invokeLocal(new ExecuteJobOperation(getName(), job.getDag())).get();
        } catch (InterruptedException | ExecutionException e) {
            throw unchecked(e);
        }
    }

    private <T> Future<T> invokeLocal(Operation op) {
        return getOperationService()
                .createInvocationBuilder(JetService.SERVICE_NAME, op, getNodeEngine().getThisAddress())
                .<T>invoke();
    }

    private <T> List<T> invokeOnCluster(Supplier<Operation> supplier) {
        final OperationService operationService = getOperationService();
        final Set<Member> members = getNodeEngine().getClusterService().getMembers();
        final List<ICompletableFuture<?>> futures = members
                .stream()
                .map(member -> operationService
                        .createInvocationBuilder(JetService.SERVICE_NAME, supplier.get(), member.getAddress())
                        .invoke())
                .collect(toList());
        return futures.stream()
                      .map(JetUtil::uncheckedGet)
                      .map(x -> (T) x)
                      .collect(toList());
    }

    private void toExecutableDag(DAG dag) {
        for (Vertex v : dag) {
            v.getSupplier();
            for (Edge e : dag.getInboundEdges(v)) {
            }
        }
    }

    private void invokeDeployment(final Set<DeploymentConfig> resources) {
        new ChunkIterator(resources, DEFAULT_RESOURCE_CHUNK_SIZE).forEachRemaining(
                chunk -> invokeOnCluster(() -> new DeployChunkOperation(name, chunk))
        );
        resources.forEach(
                r -> invokeOnCluster(() -> new UpdateDeploymentCatalogOperation(name, r.getDescriptor()))
        );
    }
}

