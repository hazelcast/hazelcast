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
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.instance.JetBuildInfo;
import com.hazelcast.internal.cluster.MemberInfo;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.core.TopologyChangedException;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.impl.execution.TaskletExecutionService;
import com.hazelcast.jet.impl.execution.init.ExecutionPlan;
import com.hazelcast.jet.impl.util.ExceptionUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.CanCancelOperations;
import com.hazelcast.spi.ConfigurableService;
import com.hazelcast.spi.LiveOperations;
import com.hazelcast.spi.LiveOperationsTracker;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.MemberAttributeServiceEvent;
import com.hazelcast.spi.MembershipAwareService;
import com.hazelcast.spi.MembershipServiceEvent;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.PacketHandler;

import java.io.IOException;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import java.util.concurrent.atomic.AtomicInteger;

public class JetService
        implements ManagedService, ConfigurableService<JetConfig>, PacketHandler, LiveOperationsTracker,
        CanCancelOperations, MembershipAwareService {

    public static final String SERVICE_NAME = "hz:impl:jetService";

    private final NodeEngineImpl nodeEngine;
    private final ILogger logger;
    private final ClientInvocationRegistry clientInvocationRegistry;
    private final LiveOperationRegistry liveOperationRegistry;

    private JetConfig config;
    private JetInstance jetInstance;
    private Networking networking;
    private TaskletExecutionService taskletExecutionService;
    private JobRepository jobRepository;
    private SnapshotRepository snapshotRepository;
    private JobCoordinationService jobCoordinationService;
    private JobExecutionService jobExecutionService;

    private final AtomicInteger numConcurrentPutAllOps = new AtomicInteger();

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
        if (config == null) {
            throw new IllegalStateException("JetConfig is not initialized");
        }

        jetInstance = new JetInstanceImpl((HazelcastInstanceImpl) engine.getHazelcastInstance(), config);
        taskletExecutionService = new TaskletExecutionService(nodeEngine.getHazelcastInstance(),
                config.getInstanceConfig().getCooperativeThreadCount());

        snapshotRepository = new SnapshotRepository(jetInstance);
        jobRepository = new JobRepository(jetInstance, snapshotRepository);

        jobExecutionService = new JobExecutionService(nodeEngine, taskletExecutionService);
        jobCoordinationService = new JobCoordinationService(nodeEngine, config, jobRepository,
                jobExecutionService, snapshotRepository);
        networking = new Networking(engine, jobExecutionService, config.getInstanceConfig().getFlowControlPeriodMs());

        ClientEngineImpl clientEngine = engine.getService(ClientEngineImpl.SERVICE_NAME);
        ExceptionUtil.registerJetExceptions(clientEngine.getClientExceptionFactory());

        jobCoordinationService.init();

        JetBuildInfo jetBuildInfo = BuildInfoProvider.getBuildInfo().getJetBuildInfo();
        logger.info(String.format("Starting Jet %s (%s - %s)",
                jetBuildInfo.getVersion(), jetBuildInfo.getBuild(), jetBuildInfo.getRevision()));
        logger.info("Setting number of cooperative threads and default parallelism to "
                + config.getInstanceConfig().getCooperativeThreadCount());

        logger.info('\n' +
                "\to   o   o   o---o o---o o     o---o   o   o---o o-o-o        o o---o o-o-o\n" +
                "\t|   |  / \\     /  |     |     |      / \\  |       |          | |       |  \n" +
                "\to---o o---o   o   o-o   |     o     o---o o---o   |          | o-o     |  \n" +
                "\t|   | |   |  /    |     |     |     |   |     |   |      \\   | |       |  \n" +
                "\to   o o   o o---o o---o o---o o---o o   o o---o   o       o--o o---o   o   ");
        logger.info("Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.");
    }

    @Override
    public void shutdown(boolean terminate) {
        jobExecutionService.reset("shutdown", HazelcastInstanceNotActiveException::new);
        networking.shutdown();
        taskletExecutionService.shutdown();
    }

    @Override
    public void reset() {
        jobCoordinationService.reset();
        jobExecutionService.reset("reset", TopologyChangedException::new);
    }

    public void initExecution(
            long jobId, long executionId, Address coordinator, int coordinatorMemberListVersion,
            Set<MemberInfo> participants, ExecutionPlan plan
    ) {
        jobExecutionService.initExecution(
                jobId, executionId, coordinator, coordinatorMemberListVersion, participants, plan
        );
    }

    public CompletionStage<Void> execute(
            Address coordinator, long jobId, long executionId,
            Consumer<CompletionStage<Void>> doneCallback
    ) {
        return jobExecutionService.execute(coordinator, jobId, executionId, doneCallback);
    }

    public void completeExecution(long executionId, Throwable error) {
        jobExecutionService.completeExecution(executionId, error);
    }

    public JobStatus getJobStatus(long jobId) {
        return jobCoordinationService.getJobStatus(jobId);
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

    public JobRepository getJobRepository() {
        return jobRepository;
    }

    public JobCoordinationService getJobCoordinationService() {
        return jobCoordinationService;
    }

    public JobExecutionService getJobExecutionService() {
        return jobExecutionService;
    }

    public ClassLoader getClassLoader(long jobId) {
        return jobCoordinationService.getClassLoader(jobId);
    }

    @Override
    public void populate(LiveOperations liveOperations) {
        liveOperationRegistry.populate(liveOperations);
    }

    @Override
    public boolean cancelOperation(Address caller, long callId) {
        return liveOperationRegistry.cancel(caller, callId);
    }

    @Override
    public void handle(Packet packet) throws IOException {
        networking.handle(packet);
    }

    @Override
    public void memberRemoved(MembershipServiceEvent event) {
        Address address = event.getMember().getAddress();
        jobExecutionService.onMemberLeave(address);
    }

    @Override
    public void memberAdded(MembershipServiceEvent event) {
    }

    @Override
    public void memberAttributeChanged(MemberAttributeServiceEvent event) {
    }

    public CompletableFuture<Boolean> submitJob(long jobId, Data dag, JobConfig config) {
        return jobCoordinationService.submitOrJoinJob(jobId, dag, config);
    }

    public CompletableFuture<Boolean> joinSubmittedJob(long jobId) {
        return jobCoordinationService.joinSubmittedJob(jobId);
    }

    public Set<Long> getAllJobIds() {
        return jobCoordinationService.getAllJobIds();
    }

    public AtomicInteger numConcurrentPutAllOps() {
        return numConcurrentPutAllOps;
    }
}
