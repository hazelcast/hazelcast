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
import com.hazelcast.jet2.Vertex;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static com.hazelcast.jet.impl.util.JetUtil.unchecked;

public class JetEngineImpl extends AbstractDistributedObject<JetService> implements JetEngine {

    private final String name;
    private final ILogger logger;

    protected JetEngineImpl(String name, NodeEngine nodeEngine, JetService service) {
        super(nodeEngine, service);
        this.name = name;
        this.logger = nodeEngine.getLogger(JetEngine.class);
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
        executeOperation(new ExecuteJobOperation(getName(), job.getDag()));
    }

    public void executeLocal(DAG dag) {
        for (Vertex vertex : dag) {
            for (int i = 0; i < vertex.getParallelism(); i++) {
                dag.getInputEdges(vertex);
                // 1 tasklet per parallelism of vertex
                // n*m boundedqueues (1-1) where n producers, m consumers - overall storage can be fixed as
                // implemented in hot restart
                // in a cluster , only one queue per node

            }
        }
    }

    private <T> List<T> executeOperation(Operation operation) {
        ClusterService clusterService = getNodeEngine().getClusterService();

        List<ICompletableFuture<T>> futures = new ArrayList<>();
        for (Member member : clusterService.getMembers()) {
            InternalCompletableFuture<T> future = getOperationService()
                    .createInvocationBuilder(JetService.SERVICE_NAME, operation, member.getAddress())
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
}

