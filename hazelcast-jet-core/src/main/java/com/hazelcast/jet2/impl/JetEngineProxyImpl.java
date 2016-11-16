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

import com.hazelcast.core.Member;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.jet2.DAG;
import com.hazelcast.jet2.JetEngine;
import com.hazelcast.jet2.JetEngineConfig;
import com.hazelcast.jet2.Job;
import com.hazelcast.jet2.ResourceConfig;
import com.hazelcast.jet2.impl.deployment.ResourceCompleteOperation;
import com.hazelcast.jet2.impl.deployment.ResourceIterator;
import com.hazelcast.jet2.impl.deployment.ResourceUpdateOperation;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.hazelcast.util.ExceptionUtil.rethrow;

public class JetEngineProxyImpl extends AbstractDistributedObject<JetService> implements JetEngineProxy {

    private final String name;
    private final ILogger logger;
    private final EngineContext engineContext;

    JetEngineProxyImpl(String name, NodeEngine nodeEngine, JetService service) {
        super(nodeEngine, service);
        this.name = name;
        this.logger = nodeEngine.getLogger(JetEngine.class);
        engineContext = service.getEngineContext(name);
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

    @Override
    public void execute(JobImpl job) {
        try {
            invokeLocal(new ExecuteJobOperation(getName(), job.getDag())).get();
        } catch (InterruptedException | ExecutionException e) {
            throw rethrow(e);
        }
    }

    private void deployResources() {
        final Set<ResourceConfig> resources = engineContext.getConfig().getResourceConfigs();
        new ResourceIterator(resources, engineContext.getConfig().getResourcePartSize()).forEachRemaining(part ->
                invokeOnCluster(getNodeEngine(), () -> new ResourceUpdateOperation(name, part)));
        resources.forEach(r ->
                invokeOnCluster(getNodeEngine(), () -> new ResourceCompleteOperation(name, r.getDescriptor())));
    }

    private <T> Future<T> invokeLocal(Operation op) {
        return getOperationService()
                .createInvocationBuilder(JetService.SERVICE_NAME, op, getNodeEngine().getThisAddress())
                .<T>invoke();
    }

    public static JetEngineProxy createEngine(String name, JetEngineConfig config, HazelcastInstanceImpl instanceImpl) {
        NodeEngineImpl nodeEngine = instanceImpl.node.getNodeEngine();
        invokeOnCluster(nodeEngine, () -> new CreateEngineIfAbsentOperation(name, config));
        JetEngineProxyImpl jetEngine = instanceImpl.getDistributedObject(JetService.SERVICE_NAME, name);
        //TODO:  check if engine already exists or not to avoid deploying resources each time
        jetEngine.deployResources();
        return jetEngine;
    }

    private static <T> List<T> invokeOnCluster(NodeEngine nodeEngine, Supplier<Operation> supplier) {
        final OperationService operationService = nodeEngine.getOperationService();
        final Set<Member> members = nodeEngine.getClusterService().getMembers();
        return members.stream()
                      .map(member -> operationService
                              .createInvocationBuilder(JetService.SERVICE_NAME, supplier.get(), member.getAddress())
                              .<T>invoke())
                      .map(Util::uncheckedGet).collect(Collectors.toList());
    }

}

