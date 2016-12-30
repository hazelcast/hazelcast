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

import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.Member;
import com.hazelcast.jet.DAG;
import com.hazelcast.jet.JetEngineConfig;
import com.hazelcast.jet.impl.deployment.JetClassLoader;
import com.hazelcast.jet.impl.deployment.ResourceStore;
import com.hazelcast.jet.impl.execution.ExecutionContext;
import com.hazelcast.jet.impl.execution.ExecutionService;
import com.hazelcast.jet.impl.execution.init.ExecutionPlan;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.SimpleExecutionCallback;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class EngineContext {

    // Type of variable is CHM and not ConcurrentMap because we rely on specific semantics of computeIfAbsent.
    // ConcurrentMap.computeIfAbsent does not guarantee at most one computation per key.
    final ConcurrentHashMap<Long, ExecutionContext> executionContexts = new ConcurrentHashMap<>();
    // keeps track of active invocations from client for cancellation support
    private final ConcurrentHashMap<Long, ICompletableFuture<Object>> clientInvocations = new ConcurrentHashMap<>();
    private final String name;
    private final ClassLoader classloader;
    private NodeEngine nodeEngine;
    private ExecutionService executionService;
    private ResourceStore resourceStore;
    private JetEngineConfig config;

    public EngineContext(String name, NodeEngine nodeEngine, JetEngineConfig config) {
        this.name = name;
        this.nodeEngine = nodeEngine;
        this.config = config;
        this.resourceStore = new ResourceStore(config.getResourceDirectory());
        this.classloader = AccessController.doPrivileged(
                (PrivilegedAction<ClassLoader>) () -> new JetClassLoader(resourceStore));
        this.executionService = new ExecutionService(nodeEngine.getHazelcastInstance(), name, config);
    }

    public String getName() {
        return name;
    }

    public NodeEngine getNodeEngine() {
        return nodeEngine;
    }

    public ResourceStore getResourceStore() {
        return resourceStore;
    }

    public ClassLoader getClassLoader() {
        return classloader;
    }

    public JetEngineConfig getConfig() {
        return config;
    }

    public void registerClientInvocation(long executionId, ICompletableFuture<Object> invocation) {
        if (clientInvocations.putIfAbsent(executionId, invocation) != null) {
            throw new IllegalStateException("Execution with id " + executionId + " is already registered.");
        }
        invocation.andThen(new SimpleExecutionCallback<Object>() {
            @Override
            public void notify(Object o) {
                clientInvocations.remove(executionId);
            }
        });
    }

    public void cancelClientInvocation(long executionId) {
        Optional.of(clientInvocations.get(executionId)).ifPresent(f -> f.cancel(true));
    }

    public void destroy() {
        resourceStore.destroy();
        executionService.shutdown();
    }

    public Map<Member, ExecutionPlan> createExecutionPlans(DAG dag) {
        return ExecutionPlan.createExecutionPlans(nodeEngine, dag, config.getParallelism());
    }

    public void initExecution(long executionId, ExecutionPlan plan) {
        final ExecutionContext[] created = {null};
        try {
            executionContexts.compute(executionId, (k, v) -> {
                if (v != null) {
                    throw new IllegalStateException("Execution context " + executionId + " already exists");
                }
                return (created[0] = new ExecutionContext(executionId, this)).initialize(plan);
            });
        } catch (Throwable t) {
            if (created[0] != null) {
                executionContexts.put(executionId, created[0]);
            }
            throw t;
        }
    }

    public void completeExecution(long executionId, Throwable error) {
        ExecutionContext context = executionContexts.remove(executionId);
        if (context != null) {
            context.complete(error);
        }
    }

    public ExecutionContext getExecutionContext(long id) {
        return executionContexts.get(id);
    }

    public ExecutionService getExecutionService() {
        return executionService;
    }

}
