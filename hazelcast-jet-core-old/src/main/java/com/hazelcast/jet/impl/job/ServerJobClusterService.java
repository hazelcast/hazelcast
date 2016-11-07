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

package com.hazelcast.jet.impl.job;


import com.hazelcast.core.Member;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.counters.Accumulator;
import com.hazelcast.jet.DAG;
import com.hazelcast.jet.impl.job.deployment.Chunk;
import com.hazelcast.jet.impl.operation.DeployChunkOperation;
import com.hazelcast.jet.impl.operation.FinishDeploymentOperation;
import com.hazelcast.jet.impl.operation.GetAccumulatorsOperation;
import com.hazelcast.jet.impl.operation.JetOperation;
import com.hazelcast.jet.impl.operation.JobEventOperation;
import com.hazelcast.jet.impl.operation.JobExecuteOperation;
import com.hazelcast.jet.impl.operation.JobInitOperation;
import com.hazelcast.jet.impl.operation.JobInterruptOperation;
import com.hazelcast.jet.impl.operation.JobSubmitOperation;
import com.hazelcast.jet.impl.statemachine.job.JobEvent;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.NodeEngine;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.Supplier;


public class ServerJobClusterService extends JobClusterService<JetOperation> {
    private final NodeEngine nodeEngine;

    public ServerJobClusterService(String name, NodeEngine nodeEngine) {
        super(name);
        this.nodeEngine = nodeEngine;
    }

    public JetOperation createInitJobInvoker(JobConfig config) {
        return new JobInitOperation(name, config);
    }

    @Override
    public JetOperation createInterruptInvoker() {
        return new JobInterruptOperation(name);
    }

    @Override
    public JetOperation createExecutionInvoker() {
        return new JobExecuteOperation(name);
    }

    @Override
    public JetOperation createAccumulatorsInvoker() {
        return new GetAccumulatorsOperation(name);
    }

    @Override
    public JetOperation createSubmitInvoker(DAG dag) {
        return new JobSubmitOperation(name, dag);
    }

    @Override
    public JetOperation createDeploymentInvoker(Chunk chunk) {
        return new DeployChunkOperation(name, chunk);
    }

    @Override
    public JetOperation createFinishDeploymentInvoker() {
        return new FinishDeploymentOperation(name);
    }

    @Override
    public JetOperation createEventInvoker(JobEvent jobEvent) {
        return new JobEventOperation(name, jobEvent);
    }

    @Override
    public Set<Member> getMembers() {
        return nodeEngine.getClusterService().getMembers();
    }

    @Override
    protected JobConfig getJobConfig() {
        return jobConfig;
    }

    @Override
    protected <T> CompletableFuture<T> createInvocation(Member member, Supplier<JetOperation> factory) {
        return new ServerJobInvocation<T>(factory.get(), member.getAddress(), nodeEngine).getFuture();
    }

    @Override
    protected <T> T toObject(Data data) {
        return nodeEngine.toObject(data);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, Accumulator> readAccumulatorsResponse(Future future) throws Exception {
        return (Map<String, Accumulator>) future.get();
    }
}
