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

import com.hazelcast.core.Member;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.jet.DAG;
import com.hazelcast.jet.JetConfig;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.JobConfig;
import com.hazelcast.jet.ResourceConfig;
import com.hazelcast.jet.impl.deployment.ResourceCompleteOperation;
import com.hazelcast.jet.impl.deployment.ResourceIterator;
import com.hazelcast.jet.impl.deployment.ResourceUpdateOperation;
import com.hazelcast.jet.impl.operation.ExecuteJobOperation;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.util.function.Supplier;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;

import static java.util.stream.Collectors.toList;

public class JetInstanceImpl extends AbstractJetInstance {
    private final NodeEngine nodeEngine;
    private final JetConfig config;

    public JetInstanceImpl(HazelcastInstanceImpl hazelcastInstance) {
        this(hazelcastInstance, new JetConfig());
    }

    public JetInstanceImpl(HazelcastInstanceImpl hazelcastInstance, JetConfig config) {
        super(hazelcastInstance);
        nodeEngine = hazelcastInstance.node.getNodeEngine();
        this.config = config;
    }

    @Override
    public JetConfig getConfig() {
        return config;
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
            Operation op = new ExecuteJobOperation(executionId, dag);
            return nodeEngine.getOperationService()
                             .createInvocationBuilder(JetService.SERVICE_NAME, op, nodeEngine.getThisAddress())
                             .invoke();
        }

        private void deployResources(long executionId) {
            final Set<ResourceConfig> resources = config.getResourceConfigs();
            new ResourceIterator(resources, config.getResourcePartSize()).forEachRemaining(
                    part -> invokeOnCluster(() -> new ResourceUpdateOperation(executionId, part))
            );
            resources.forEach(r -> invokeOnCluster(() -> new ResourceCompleteOperation(executionId, r.getDescriptor())));
        }

        private <T> List<T> invokeOnCluster(Supplier<Operation> supplier) {
            final OperationService operationService = nodeEngine.getOperationService();
            final Set<Member> members = nodeEngine.getClusterService().getMembers();
            return members.stream()
                          .map(member -> operationService
                                  .createInvocationBuilder(JetService.SERVICE_NAME, supplier.get(), member.getAddress())
                                  .<T>invoke())
                          .collect(toList())
                          .stream()
                          .map(Util::uncheckedGet)
                          .collect(toList());
        }


    }
}
