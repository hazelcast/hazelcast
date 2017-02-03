/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.impl.ClientEngineImpl;
import com.hazelcast.core.Member;
import com.hazelcast.core.MigrationEvent;
import com.hazelcast.core.MigrationListener;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.instance.JetBuildInfo;
import com.hazelcast.jet.DAG;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.TopologyChangedException;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.impl.deployment.JetClassLoader;
import com.hazelcast.jet.impl.deployment.ResourceStore;
import com.hazelcast.jet.impl.execution.ExecutionContext;
import com.hazelcast.jet.impl.execution.ExecutionService;
import com.hazelcast.jet.impl.execution.init.ExecutionPlan;
import com.hazelcast.jet.impl.util.ExceptionUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Packet;
import com.hazelcast.spi.CanCancelOperations;
import com.hazelcast.spi.ConfigurableService;
import com.hazelcast.spi.LiveOperations;
import com.hazelcast.spi.LiveOperationsTracker;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.PacketHandler;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class JetService
        implements ManagedService, ConfigurableService<JetConfig>, PacketHandler, LiveOperationsTracker,
        CanCancelOperations {

    public static final String SERVICE_NAME = "hz:impl:jetService";

    private final ILogger logger;
    private final ClientInvocationRegistry clientInvocationRegistry;
    private final LiveOperationRegistry liveOperationRegistry;

    // The type of these variables is CHM and not ConcurrentMap because we rely on specific semantics of
    // computeIfAbsent. ConcurrentMap.computeIfAbsent does not guarantee at most one computation per key.
    private final ConcurrentHashMap<Long, ExecutionContext> executionContexts = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, ResourceStore> resourceStores = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, ClassLoader> classLoaders = new ConcurrentHashMap<>();

    private JetConfig config = new JetConfig();
    private NodeEngineImpl nodeEngine;
    private JetInstance jetInstance;
    private Networking networking;
    private ExecutionService executionService;


    public JetService(NodeEngine nodeEngine) {
        this.nodeEngine = (NodeEngineImpl) nodeEngine;
        this.logger = nodeEngine.getLogger(getClass());
        this.clientInvocationRegistry = new ClientInvocationRegistry();
        this.liveOperationRegistry = new LiveOperationRegistry();
    }

    @Override
    public void configure(JetConfig config) {
        this.config = config;
    }


    // ManagedService

    @Override
    public void init(NodeEngine engine, Properties properties) {
        engine.getPartitionService().addMigrationListener(new CancelJobsMigrationListener());
        jetInstance = new JetInstanceImpl((HazelcastInstanceImpl) engine.getHazelcastInstance(), config);
        networking = new Networking(engine, executionContexts, config.getInstanceConfig().getFlowControlPeriodMs());
        executionService = new ExecutionService(nodeEngine.getHazelcastInstance(),
                config.getInstanceConfig().getCooperativeThreadCount());

        ClientEngineImpl clientEngine = engine.getService(ClientEngineImpl.SERVICE_NAME);
        ExceptionUtil.registerJetExceptions(clientEngine.getClientExceptionFactory());

        JetBuildInfo jetBuildInfo = BuildInfoProvider.getBuildInfo().getJetBuildInfo();
        logger.info("Starting Jet " + jetBuildInfo.getVersion() + " (" + jetBuildInfo.getBuild() + " - " +
                jetBuildInfo.getRevision() + ") ");
        logger.info("Setting number of cooperative threads and default parallelism to "
                + config.getInstanceConfig()
                        .getCooperativeThreadCount());

        logger.info("\n" +
                "\to   o   o   o---o o---o o     o---o   o   o---o o-o-o        o o---o o-o-o\n" +
                "\t|   |  / \\     /  |     |     |      / \\  |       |          | |       |  \n" +
                "\to---o o---o   o   o-o   |     o     o---o o---o   |          | o-o     |  \n" +
                "\t|   | |   |  /    |     |     |     |   |     |   |      \\   | |       |  \n" +
                "\to   o o   o o---o o---o o---o o---o o   o o---o   o       o--o o---o   o   ");
        logger.info("Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.");
    }

    @Override
    public void shutdown(boolean terminate) {
        networking.destroy();
        executionService.shutdown();
    }

    @Override
    public void reset() {
    }

    // End ManagedService


    public void initExecution(long executionId, ExecutionPlan plan) {
        final ExecutionContext[] created = {null};
        try {
            executionContexts.compute(executionId, (k, v) -> {
                if (v != null) {
                    throw new IllegalStateException("Execution context " + executionId + " already exists");
                }
                return (created[0] = new ExecutionContext(executionId, nodeEngine, executionService)).initialize(plan);
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

    public JetInstance getJetInstance() {
        return jetInstance;
    }

    public LiveOperationRegistry getLiveOperationRegistry() {
        return liveOperationRegistry;
    }

    public ClientInvocationRegistry getClientInvocationRegistry() {
        return clientInvocationRegistry;
    }

    public ResourceStore getResourceStore(long executionId) {
        return resourceStores.computeIfAbsent(executionId, (k) -> new ResourceStore(config.getInstanceConfig().getTempDir()));
    }

    public ClassLoader getClassLoader(long executionId) {
        return classLoaders.computeIfAbsent(executionId, (k) -> AccessController.doPrivileged(
                (PrivilegedAction<ClassLoader>) () -> new JetClassLoader(getResourceStore(k))
        ));
    }

    public ExecutionContext getExecutionContext(long executionId) {
        return executionContexts.get(executionId);
    }

    public Map<Member, ExecutionPlan> createExecutionPlans(DAG dag) {
        return ExecutionPlan.createExecutionPlans(nodeEngine, dag, config.getInstanceConfig().getCooperativeThreadCount());
    }


    // LiveOperationsTracker

    @Override
    public void populate(LiveOperations liveOperations) {
        liveOperationRegistry.populate(liveOperations);
    }

    @Override
    public boolean cancelOperation(Address caller, long callId) {
        return liveOperationRegistry.cancel(caller, callId);
    }


    // PacketHandler

    @Override
    public void handle(Packet packet) throws IOException {
        networking.handle(packet);
    }


    private class CancelJobsMigrationListener implements MigrationListener {

        @Override
        public void migrationStarted(MigrationEvent migrationEvent) {
            Set<Address> addresses = nodeEngine.getClusterService().getMembers().stream()
                                               .map(Member::getAddress)
                                               .collect(Collectors.toSet());
            // complete the processors, whose caller is dead, with TopologyChangedException
            liveOperationRegistry.liveOperations
                    .entrySet().stream()
                    .filter(e -> !addresses.contains(e.getKey()))
                    .flatMap(e -> e.getValue().values().stream())
                    .forEach(op ->
                            Optional.ofNullable(executionContexts.get(op.getExecutionId()))
                                    .map(ExecutionContext::getExecutionCompletionStage)
                                    .ifPresent(stage -> stage.whenComplete((aVoid, throwable) ->
                                            completeExecution(op.getExecutionId(),
                                                    new TopologyChangedException("Topology has been changed")))));
            // send exception result to all operations
            liveOperationRegistry.liveOperations
                    .values().stream().map(Map::values).flatMap(Collection::stream)
                    .forEach(op -> op.completeExceptionally(new TopologyChangedException("Topology has been changed")));
        }

        @Override
        public void migrationCompleted(MigrationEvent migrationEvent) {
        }

        @Override
        public void migrationFailed(MigrationEvent migrationEvent) {
        }
    }

}
