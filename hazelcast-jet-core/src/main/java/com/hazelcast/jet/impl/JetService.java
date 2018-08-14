/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.instance.JetBuildInfo;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.JobNotFoundException;
import com.hazelcast.jet.impl.execution.TaskletExecutionService;
import com.hazelcast.jet.impl.operation.NotifyMemberShutdownOperation;
import com.hazelcast.jet.impl.util.ExceptionUtil;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Packet;
import com.hazelcast.spi.ConfigurableService;
import com.hazelcast.spi.LiveOperations;
import com.hazelcast.spi.LiveOperationsTracker;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.MemberAttributeServiceEvent;
import com.hazelcast.spi.MembershipAwareService;
import com.hazelcast.spi.MembershipServiceEvent;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.PacketHandler;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static java.util.concurrent.TimeUnit.SECONDS;

public class JetService
        implements ManagedService, ConfigurableService<JetConfig>, PacketHandler, MembershipAwareService,
        LiveOperationsTracker {

    public static final int MAX_PARALLEL_ASYNC_OPS = 1000;

    public static final String SERVICE_NAME = "hz:impl:jetService";
    private static final int NOTIFY_MEMBER_SHUTDOWN_DELAY = 5;

    private final NodeEngineImpl nodeEngine;
    private final ILogger logger;
    private final LiveOperationRegistry liveOperationRegistry;
    private final AtomicBoolean shutdownInitiated = new AtomicBoolean();

    private JetConfig config;
    private JetInstance jetInstance;
    private Networking networking;
    private TaskletExecutionService taskletExecutionService;
    private JobRepository jobRepository;
    private JobCoordinationService jobCoordinationService;
    private JobExecutionService jobExecutionService;

    private final AtomicInteger numConcurrentAsyncOps = new AtomicInteger();

    private final Supplier<int[]> sharedPartitionKeys = Util.memoizeConcurrent(this::computeSharedPartitionKeys);

    public JetService(NodeEngine nodeEngine) {
        this.nodeEngine = (NodeEngineImpl) nodeEngine;
        this.logger = nodeEngine.getLogger(getClass());
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
        taskletExecutionService = new TaskletExecutionService(nodeEngine,
                config.getInstanceConfig().getCooperativeThreadCount());

        SnapshotRepository snapshotRepository = new SnapshotRepository(jetInstance);
        jobRepository = new JobRepository(jetInstance, snapshotRepository);

        jobExecutionService = new JobExecutionService(nodeEngine, taskletExecutionService, jobRepository);
        jobCoordinationService = new JobCoordinationService(nodeEngine, this, config, jobRepository,
                snapshotRepository);
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
        logger.info("Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.");
    }

    /**
     * Gracefully shuts down jobs on this member. Blocks until all are down.
     */
    void shutDownJobs() {
        if (!shutdownInitiated.compareAndSet(false, true)) {
            logger.info("Shutdown requested, but already shut down or being shut down");
            return;
        }
        // this will prevent accepting more jobs
        jobCoordinationService.shutdown();
        jobExecutionService.shutdown(true);
        taskletExecutionService.shutdown(true);

        CompletableFuture<Void> future = new CompletableFuture<>();
        notifyMasterWeAreShuttingDown(future);
        // We initiated shutdown on this member, it won't accept any new jobs. After all
        // tasklets running locally are done, we can continue the shutdown.
        taskletExecutionService.awaitWorkerTermination();
        future.join();
    }

    private void notifyMasterWeAreShuttingDown(CompletableFuture<Void> result) {
        Operation op = new NotifyMemberShutdownOperation();
        nodeEngine.getOperationService()
                  .invokeOnTarget(JetService.SERVICE_NAME, op, nodeEngine.getClusterService().getMasterAddress())
                  .andThen(new ExecutionCallback<Object>() {
                      @Override
                      public void onResponse(Object response) {
                          result.complete(null);
                      }

                      @Override
                      public void onFailure(Throwable t) {
                          logger.warning("Failed to notify master member that this member is shutting down, will retry in "
                                  + NOTIFY_MEMBER_SHUTDOWN_DELAY + " seconds");
                          // recursive call
                          nodeEngine.getExecutionService().schedule(
                                  () -> notifyMasterWeAreShuttingDown(result), NOTIFY_MEMBER_SHUTDOWN_DELAY, SECONDS);
                      }
                  });
    }

    @Override
    public void shutdown(boolean forceful) {
        jobCoordinationService.shutdown();
        jobExecutionService.shutdown(false);
        taskletExecutionService.shutdown(false);
        taskletExecutionService.awaitWorkerTermination();
        networking.shutdown();
    }

    @Override
    public void reset() {
        jobExecutionService.reset();
        jobCoordinationService.reset();
    }

    public JetInstance getJetInstance() {
        return jetInstance;
    }

    public LiveOperationRegistry getLiveOperationRegistry() {
        return liveOperationRegistry;
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

    /**
     * Returns the job config or fails with {@link JobNotFoundException}
     * if the requested job is not found.
     */
    public JobConfig getJobConfig(long jobId) {
        JobRecord jobRecord = jobRepository.getJobRecord(jobId);
        if (jobRecord != null) {
            return jobRecord.getConfig();
        }

        JobResult jobResult = jobRepository.getJobResult(jobId);
        if (jobResult != null) {
            return jobResult.getJobConfig();
        }

        throw new JobNotFoundException(jobId);
    }

    public ClassLoader getClassLoader(long jobId) {
        return getJobExecutionService().getClassLoader(getJobConfig(jobId), jobId);
    }

    @Override
    public void handle(Packet packet) throws IOException {
        networking.handle(packet);
    }

    @Override
    public void memberRemoved(MembershipServiceEvent event) {
        jobExecutionService.onMemberLeave(event.getMember().getAddress());
        jobCoordinationService.onMemberLeave(event.getMember().getUuid());
    }

    @Override
    public void memberAdded(MembershipServiceEvent event) {
        jobCoordinationService.onMemberAdded(event.getMember());
    }

    @Override
    public void memberAttributeChanged(MemberAttributeServiceEvent event) {
    }

    public AtomicInteger numConcurrentAsyncOps() {
        return numConcurrentAsyncOps;
    }

    @Override
    public void populate(LiveOperations liveOperations) {
        liveOperationRegistry.populate(liveOperations);
    }

    /**
     * Returns an array of pre-generated keys, one for each partition. At index
     * <em>i</em> there's a key, that we know will go to partition <em>i</em>.
     */
    public int[] getSharedPartitionKeys() {
        return sharedPartitionKeys.get();
    }

    private int[] computeSharedPartitionKeys() {
        InternalPartitionService partitionService = nodeEngine.getPartitionService();
        int[] keys = new int[partitionService.getPartitionCount()];
        int remainingCount = partitionService.getPartitionCount();
        for (int i = 1; remainingCount > 0; i++) {
            int partitionId = partitionService.getPartitionId(i);
            if (keys[partitionId] == 0) {
                keys[partitionId] = i;
                remainingCount--;
            }
        }
        return keys;
    }
}
