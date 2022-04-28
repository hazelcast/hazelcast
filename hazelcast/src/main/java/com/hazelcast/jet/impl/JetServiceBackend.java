/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.impl.ClientEngine;
import com.hazelcast.client.impl.ClientEngineImpl;
import com.hazelcast.client.impl.protocol.ClientExceptionFactory;
import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.instance.impl.NodeState;
import com.hazelcast.internal.cluster.ClusterStateListener;
import com.hazelcast.internal.metrics.impl.MetricsService;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.services.ManagedService;
import com.hazelcast.internal.services.MembershipAwareService;
import com.hazelcast.internal.services.MembershipServiceEvent;
import com.hazelcast.jet.JetService;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.JobNotFoundException;
import com.hazelcast.jet.impl.execution.TaskletExecutionService;
import com.hazelcast.jet.impl.metrics.JobMetricsPublisher;
import com.hazelcast.jet.impl.operation.NotifyMemberShutdownOperation;
import com.hazelcast.jet.impl.operation.PrepareForPassiveClusterOperation;
import com.hazelcast.jet.impl.serialization.DelegatingSerializationService;
import com.hazelcast.jet.impl.util.ExceptionUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.LiveOperations;
import com.hazelcast.spi.impl.operationservice.LiveOperationsTracker;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.merge.DiscardMergePolicy;
import com.hazelcast.spi.properties.HazelcastProperties;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static com.hazelcast.cluster.ClusterState.PASSIVE;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.jet.impl.JobRepository.INTERNAL_JET_OBJECTS_PREFIX;
import static com.hazelcast.jet.impl.JobRepository.JOB_METRICS_MAP_NAME;
import static com.hazelcast.jet.impl.JobRepository.JOB_RESULTS_MAP_NAME;
import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.jet.impl.util.Util.memoizeConcurrent;
import static com.hazelcast.spi.properties.ClusterProperty.JOB_RESULTS_TTL_SECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class JetServiceBackend implements ManagedService, MembershipAwareService, ClusterStateListener,
        LiveOperationsTracker, Consumer<Packet> {

    public static final String SERVICE_NAME = "hz:impl:jetService";
    public static final int MAX_PARALLEL_ASYNC_OPS = 1000;

    private static final int NOTIFY_MEMBER_SHUTDOWN_DELAY = 5;
    private static final int SHUTDOWN_JOBS_MAX_WAIT_SECONDS = 10;

    private NodeEngineImpl nodeEngine;
    private final ILogger logger;
    private final LiveOperationRegistry liveOperationRegistry;
    private final AtomicReference<CompletableFuture<Void>> shutdownFuture = new AtomicReference<>();
    private final JetConfig jetConfig;

    private JetService jet;
    private Networking networking;
    private TaskletExecutionService taskletExecutionService;
    private JobRepository jobRepository;
    private JobCoordinationService jobCoordinationService;
    private JobClassLoaderService jobClassLoaderService;
    private JobExecutionService jobExecutionService;

    private final AtomicInteger numConcurrentAsyncOps = new AtomicInteger();
    private final Supplier<int[]> sharedPartitionKeys = memoizeConcurrent(this::computeSharedPartitionKeys);

    public JetServiceBackend(Node node) {
        this.logger = node.getLogger(getClass());
        this.liveOperationRegistry = new LiveOperationRegistry();
        this.jetConfig = node.getConfig().getJetConfig();
    }

    // ManagedService
    @Override
    public void init(NodeEngine engine, Properties hzProperties) {
        this.nodeEngine = (NodeEngineImpl) engine;
        this.jet = new JetInstanceImpl(nodeEngine.getNode().hazelcastInstance, jetConfig);
        jobRepository = new JobRepository(engine.getHazelcastInstance());
        taskletExecutionService = new TaskletExecutionService(
                nodeEngine, jetConfig.getCooperativeThreadCount(), nodeEngine.getProperties()
        );
        jobCoordinationService = createJobCoordinationService();
        jobClassLoaderService = new JobClassLoaderService(nodeEngine, jobRepository);
        jobExecutionService = new JobExecutionService(nodeEngine, taskletExecutionService, jobClassLoaderService);

        MetricsService metricsService = nodeEngine.getService(MetricsService.SERVICE_NAME);
        metricsService.registerPublisher(nodeEngine -> new JobMetricsPublisher(jobExecutionService,
                nodeEngine.getLocalMember()));
        nodeEngine.getMetricsRegistry().registerDynamicMetricsProvider(jobExecutionService);
        networking = new Networking(engine, jobExecutionService, jetConfig.getFlowControlPeriodMs());

        ClientEngine clientEngine = engine.getService(ClientEngineImpl.SERVICE_NAME);
        ClientExceptionFactory clientExceptionFactory = clientEngine.getExceptionFactory();
        if (clientExceptionFactory != null) {
            ExceptionUtil.registerJetExceptions(clientExceptionFactory);
        } else {
            logger.fine("Jet exceptions are not registered to the ClientExceptionFactory" +
                    " since the ClientExceptionFactory is not accessible.");
        }
        logger.info("Setting number of cooperative threads and default parallelism to "
                + jetConfig.getCooperativeThreadCount());
    }

    public void configureJetInternalObjects(Config config, HazelcastProperties properties) {
        JetConfig jetConfig = config.getJetConfig();
        MapConfig internalMapConfig = new MapConfig(INTERNAL_JET_OBJECTS_PREFIX + '*')
                .setBackupCount(jetConfig.getBackupCount())
                // we query creationTime of resources maps
                .setStatisticsEnabled(true);

        internalMapConfig.getMergePolicyConfig().setPolicy(DiscardMergePolicy.class.getName());

        MapConfig resultsMapConfig = new MapConfig(internalMapConfig)
                .setName(JOB_RESULTS_MAP_NAME)
                .setTimeToLiveSeconds(properties.getSeconds(JOB_RESULTS_TTL_SECONDS));

        MapConfig metricsMapConfig = new MapConfig(internalMapConfig)
                .setName(JOB_METRICS_MAP_NAME)
                .setTimeToLiveSeconds(properties.getSeconds(JOB_RESULTS_TTL_SECONDS));

        config.addMapConfig(internalMapConfig)
                .addMapConfig(resultsMapConfig)
                .addMapConfig(metricsMapConfig);
    }

    /**
     * Tells master to gracefully terminate jobs on this member. Blocks until
     * all are down.
     */
    public void shutDownJobs() {
        if (shutdownFuture.compareAndSet(null, new CompletableFuture<>())) {
            notifyMasterWeAreShuttingDown(shutdownFuture.get());
        }
        try {
            CompletableFuture<Void> future = shutdownFuture.get();
            future.get(SHUTDOWN_JOBS_MAX_WAIT_SECONDS, SECONDS);
            // Note that at this point there can still be executions running - those for light jobs
            // or those created automatically after a packet was received.
            // They are all non-fault-tolerant or contain only the packets, that will be dropped
            // when this member actually shuts down.
        } catch (Exception e) {
            logger.severe("Shutdown jobs timeout", e);
        }
    }

    private void notifyMasterWeAreShuttingDown(CompletableFuture<Void> future) {
        Operation op = new NotifyMemberShutdownOperation();
        nodeEngine.getOperationService()
                  .invokeOnTarget(JetServiceBackend.SERVICE_NAME, op, nodeEngine.getClusterService().getMasterAddress())
                  .whenCompleteAsync((response, throwable) -> {
                      // if there is an error and the node is still ACTIVE, try again. If the node isn't ACTIVE, log & ignore.
                      NodeState nodeState = nodeEngine.getNode().getState();
                      if (throwable != null && nodeState == NodeState.ACTIVE) {
                          logger.warning("Failed to notify master member that this member is shutting down," +
                                  " will retry in " + NOTIFY_MEMBER_SHUTDOWN_DELAY + " seconds", throwable);
                          // recursive call
                          nodeEngine.getExecutionService().schedule(
                                  () -> notifyMasterWeAreShuttingDown(future), NOTIFY_MEMBER_SHUTDOWN_DELAY, SECONDS);
                      } else {
                          if (throwable != null) {
                              logger.warning("Failed to notify master member that this member is shutting down," +
                                      " but this member is " + nodeState + ", so not retrying", throwable);
                          }
                          future.complete(null);
                      }
                  });
    }

    @Override
    public void shutdown(boolean forceful) {
        jobExecutionService.shutdown();
        taskletExecutionService.shutdown();
        taskletExecutionService.awaitWorkerTermination();
        networking.shutdown();
    }

    @Override
    public void reset() {
        jobExecutionService.reset();
        jobCoordinationService.reset();
    }

    // Overridden in EE with EnterpriseJobCoordinationService
    JobCoordinationService createJobCoordinationService() {
        return new JobCoordinationService(nodeEngine, this, jetConfig, jobRepository);
    }

    public InternalSerializationService createSerializationService(Map<String, String> serializerConfigs) {
        return DelegatingSerializationService
                .from(getNodeEngine().getSerializationService(), serializerConfigs);
    }

    @SuppressWarnings("unused") // parameters are used from jet-enterprise
    public Operation createExportSnapshotOperation(long jobId, String name, boolean cancelJob) {
        throw new UnsupportedOperationException("You need Hazelcast Enterprise to use this feature");
    }

    public JetService getJet() {
        return this.jet;
    }

    public LiveOperationRegistry getLiveOperationRegistry() {
        return liveOperationRegistry;
    }

    public JobRepository getJobRepository() {
        return jobRepository;
    }

    public NodeEngineImpl getNodeEngine() {
        return nodeEngine;
    }

    public JetConfig getJetConfig() {
        return jetConfig;
    }

    public JobCoordinationService getJobCoordinationService() {
        return jobCoordinationService;
    }

    public JobClassLoaderService getJobClassLoaderService() {
        return jobClassLoaderService;
    }

    public JobExecutionService getJobExecutionService() {
        return jobExecutionService;
    }

    /**
     * Returns the job config or fails with {@link JobNotFoundException}
     * if the requested job is not found.
     */
    public JobConfig getJobConfig(long jobId, boolean isLightJob) {
        if (isLightJob) {
            return jobCoordinationService.getLightJobConfig(jobId);
        }

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

    @Override
    public void accept(Packet packet) {
        try {
            networking.handle(packet);
        } catch (IOException e) {
            throw sneakyThrow(e);
        }
    }

    @Override
    public void memberRemoved(MembershipServiceEvent event) {
        jobExecutionService.onMemberRemoved(event.getMember());
        jobCoordinationService.onMemberRemoved(event.getMember().getUuid());
    }

    @Override
    public void memberAdded(MembershipServiceEvent event) {
        jobCoordinationService.onMemberAdded(event.getMember());
    }

    @Override
    public void onClusterStateChange(ClusterState newState) {
        getJobCoordinationService().clusterChangeDone();
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

    public TaskletExecutionService getTaskletExecutionService() {
        return taskletExecutionService;
    }

    public void beforeClusterStateChange(ClusterState requestedState) {
        if (requestedState == PASSIVE) {
            try {
                nodeEngine.getOperationService().createInvocationBuilder(JetServiceBackend.SERVICE_NAME,
                        new PrepareForPassiveClusterOperation(), nodeEngine.getMasterAddress())
                        .invoke().get();
            } catch (InterruptedException | ExecutionException e) {
                throw rethrow(e);
            }
        }
    }

    public void startScanningForJobs() {
        jobCoordinationService.startScanningForJobs();
    }

}
