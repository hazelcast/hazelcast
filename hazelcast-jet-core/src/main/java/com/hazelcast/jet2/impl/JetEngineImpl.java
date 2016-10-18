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
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.jet2.DAG;
import com.hazelcast.jet2.JetEngine;
import com.hazelcast.jet2.Job;
import com.hazelcast.jet2.impl.deployment.ChunkIterator;
import com.hazelcast.jet2.impl.deployment.DeployChunkOperation;
import com.hazelcast.jet2.DeploymentConfig;
import com.hazelcast.jet2.impl.deployment.ResourceChunk;
import com.hazelcast.jet2.impl.deployment.UpdateDeploymentCatalogOperation;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

import static com.hazelcast.jet.impl.util.JetUtil.unchecked;

public class JetEngineImpl extends AbstractDistributedObject<JetService> implements JetEngine {

    private static final int DEFAULT_RESOURCE_CHUNK_SIZE = 16384;
    private final String name;
    private final ILogger logger;
    private final ExecutionContext executionContext;

    protected JetEngineImpl(String name, NodeEngine nodeEngine, JetService service) {
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
        invokeOperation(() -> new ExecuteJobOperation(getName(), job.getDag()));
    }

    private <T> List<T> invokeOperation(Supplier<Operation> supplier) {
        ClusterService clusterService = getNodeEngine().getClusterService();

        List<ICompletableFuture<T>> futures = new ArrayList<>();
        for (Member member : clusterService.getMembers()) {
            InternalCompletableFuture<T> future = getOperationService()
                    .createInvocationBuilder(JetService.SERVICE_NAME, supplier.get(), member.getAddress())
                    .<T>invoke();
            futures.add(future);
        }
        try {
            List<T> results = new ArrayList<>();
            for (ICompletableFuture<T> future : futures) {
                results.add(future.get());
            }
            return results;
        } catch (InterruptedException | ExecutionException e) {
            throw unchecked(e);
        }
    }

    private void invokeDeployment(final Set<DeploymentConfig> resources) {
        Iterator<ResourceChunk> iterator = new ChunkIterator(resources, DEFAULT_RESOURCE_CHUNK_SIZE);
        while (iterator.hasNext()) {
            final ResourceChunk resourceChunk = iterator.next();
            invokeOperation(() -> new DeployChunkOperation(name, resourceChunk));
        }
        for (DeploymentConfig resource : resources) {
            invokeOperation(() -> new UpdateDeploymentCatalogOperation(name, resource.getDescriptor()));
        }
    }
}

