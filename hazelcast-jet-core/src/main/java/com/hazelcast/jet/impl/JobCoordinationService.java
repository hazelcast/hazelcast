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

import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.IMap;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.JobStatus;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.impl.deployment.JetClassLoader;
import com.hazelcast.jet.impl.execution.SnapshotRecord.SnapshotStatus;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.executionservice.InternalExecutionService;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.util.Clock;

import java.security.PrivilegedAction;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.cluster.memberselector.MemberSelectors.DATA_MEMBER_SELECTOR;
import static com.hazelcast.jet.JobStatus.NOT_STARTED;
import static com.hazelcast.jet.JobStatus.RUNNING;
import static com.hazelcast.jet.impl.JobRepository.JOB_RESULTS_MAP_NAME;
import static com.hazelcast.jet.impl.execution.SnapshotRecord.SnapshotStatus.FAILED;
import static com.hazelcast.jet.impl.execution.SnapshotRecord.SnapshotStatus.SUCCESSFUL;
import static com.hazelcast.jet.impl.execution.SnapshotRecord.SnapshotStatus.VALIDATION_NEEDED;
import static com.hazelcast.jet.impl.util.JetGroupProperty.JOB_SCAN_PERIOD;
import static com.hazelcast.jet.impl.util.Util.jobAndExecutionId;
import static com.hazelcast.jet.impl.util.Util.idToString;
import static com.hazelcast.util.executor.ExecutorType.CACHED;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class JobCoordinationService {

    private static final String COORDINATOR_EXECUTOR_NAME = "jet:coordinator";
    private static final long RETRY_DELAY_IN_MILLIS = SECONDS.toMillis(2);

    private final NodeEngineImpl nodeEngine;
    private final JetConfig config;
    private final ILogger logger;
    private final JobRepository jobRepository;
    private final JobExecutionService jobExecutionService;
    private final SnapshotRepository snapshotRepository;
    private final ConcurrentMap<Long, MasterContext> masterContexts = new ConcurrentHashMap<>();
    private final IMap<Long, JobResult> jobResults;

    public JobCoordinationService(NodeEngineImpl nodeEngine, JetConfig config,
                                  JobRepository jobRepository, JobExecutionService jobExecutionService,
                                  SnapshotRepository snapshotRepository) {
        this.nodeEngine = nodeEngine;
        this.config = config;
        this.logger = nodeEngine.getLogger(getClass());
        this.jobRepository = jobRepository;
        this.jobExecutionService = jobExecutionService;
        this.snapshotRepository = snapshotRepository;
        this.jobResults = nodeEngine.getHazelcastInstance().getMap(JOB_RESULTS_MAP_NAME);
    }

    public void init() {
        InternalExecutionService executionService = nodeEngine.getExecutionService();
        HazelcastProperties properties = new HazelcastProperties(config.getProperties());
        long jobScanPeriodInMillis = properties.getMillis(JOB_SCAN_PERIOD);
        executionService.register(COORDINATOR_EXECUTOR_NAME, 2, Integer.MAX_VALUE, CACHED);
        executionService.scheduleWithRepetition(COORDINATOR_EXECUTOR_NAME, this::scanJobs,
                jobScanPeriodInMillis, jobScanPeriodInMillis, MILLISECONDS);
    }

    public void reset() {
        masterContexts.values().forEach(MasterContext::cancel);
    }

    public ClassLoader getClassLoader(long jobId) {
        PrivilegedAction<JetClassLoader> action = () -> new JetClassLoader(jobRepository.getJobResources(jobId));
        return jobExecutionService.getClassLoader(jobId, action);
    }

    // only for testing
    public Map<Long, MasterContext> getMasterContexts() {
        return new HashMap<>(masterContexts);
    }

    // only for testing
    public MasterContext getMasterContext(long jobId) {
        return masterContexts.get(jobId);
    }

    // only for testing
    public JobResult getJobResult(long jobId) {
        return jobResults.get(jobId);
    }

    // only for testing
    public CompletableFuture<Boolean> joinJobIfRunning(long jobId) {
        MasterContext masterContext = masterContexts.get(jobId);
        if (masterContext == null) {
            throw new IllegalStateException("job " + idToString(jobId) + " is not running!");
        }

        return masterContext.completionFuture();
    }

    /**
     * Starts the job if it is not already started or completed. Returns a future which represents result of the job.
     */
    public CompletableFuture<Boolean> submitOrJoinJob(long jobId, Data dag, JobConfig config) {
        if (!isMaster()) {
            throw new JetException("Job cannot be started here. Master address: "
                    + nodeEngine.getClusterService().getMasterAddress());
        }

        // the order of operations is important.

        // first, check if the job is already completed
        JobResult jobResult = jobResults.get(jobId);
        if (jobResult != null) {
            logger.fine("Not starting job " + idToString(jobId) + " since already completed with result: " +
                    jobResult);
            return jobResult.asCompletableFuture();
        }

        int quorumSize = config.isSplitBrainProtectionEnabled() ? getQuorumSize() : 0;
        JobRecord jobRecord = new JobRecord(jobId, Clock.currentTimeMillis(), dag, config, quorumSize);
        MasterContext masterContext = new MasterContext(nodeEngine, this, jobRecord);

        // just try to initiate the coordination
        MasterContext prev = masterContexts.putIfAbsent(jobId, masterContext);
        if (prev != null) {
            logger.fine("Joining to already started job " + idToString(jobId));
            return prev.completionFuture();
        }

        // If job is not currently running, it might be that it is just completed
        if (completeMasterContextIfJobAlreadyCompleted(masterContext)) {
            return masterContext.completionFuture();
        }

        // If there is no master context and job result at the same time, it means this is the first submission
        jobRepository.putNewJobRecord(jobRecord);

        logger.info("Starting job " + idToString(jobId) + " based on join/submit request from client");
        tryStartJob(masterContext);
        return masterContext.completionFuture();
    }

    public CompletableFuture<Boolean> joinSubmittedJob(long jobId) {
        if (!isMaster()) {
            throw new JetException("Cannot join to submitted job jere. Master address: "
                    + nodeEngine.getClusterService().getMasterAddress());
        }

        JobRecord jobRecord = jobRepository.getJob(jobId);
        if (jobRecord != null) {
            return submitOrJoinJob(jobId, jobRecord.getDag(), jobRecord.getConfig());
        }

        JobResult jobResult = jobResults.get(jobId);
        if (jobResult != null) {
            return jobResult.asCompletableFuture();
        }

        throw new IllegalArgumentException("Job " + idToString(jobId) + " not found to join!");
    }

    // Tries to automatically start a job if it is not already running or completed
    private void startJobIfNotStartedOrCompleted(JobRecord jobRecord) {
        // the order of operations is important.

        long jobId = jobRecord.getJobId();
        if (jobResults.containsKey(jobId) || masterContexts.containsKey(jobId)) {
            return;
        }

        MasterContext masterContext = new MasterContext(nodeEngine, this, jobRecord);
        MasterContext prev = masterContexts.putIfAbsent(jobId, masterContext);
        if (prev != null) {
            return;
        }

        // If job is not currently running, it might be that it is just completed.
        // Since we put the MasterContext into the masterContexts map, someone else could be joined to the job
        // so we should notify its future
        if (completeMasterContextIfJobAlreadyCompleted(masterContext)) {
            return;
        }

        logger.info("Starting job " + idToString(masterContext.getJobId()) + " discovered by scanning of JobRecord-s");
        tryStartJob(masterContext);
    }

    // If a job result is present, it completes the master context using the job result
    private boolean completeMasterContextIfJobAlreadyCompleted(MasterContext masterContext) {
        long jobId = masterContext.getJobId();
        JobResult jobResult = jobResults.get(jobId);
        if (jobResult != null) {
            logger.fine("Completing master context " + idToString(jobId) + " since already completed with result: " +
                    jobResult);
            masterContext.setFinalResult(jobResult.getFailure());
            return masterContexts.remove(jobId, masterContext);
        }

        return false;
    }

    private void tryStartJob(MasterContext masterContext) {
        masterContext.tryStartJob(jobRepository::newExecutionId);
    }

    private int getQuorumSize() {
        return (getDataMemberCount() / 2) + 1;
    }

    boolean isQuorumPresent(int quorumSize) {
        return getDataMemberCount() >= quorumSize;
    }

    private int getDataMemberCount() {
        ClusterService clusterService = nodeEngine.getClusterService();
        return clusterService.getMembers(DATA_MEMBER_SELECTOR).size();
    }

    public Set<Long> getAllJobIds() {
        Set<Long> jobIds = new HashSet<>(jobRepository.getJobIds());
        jobIds.addAll(masterContexts.keySet());
        jobIds.addAll(jobResults.keySet());
        return jobIds;
    }

    /**
     * Returns the job status or fails with {@link IllegalArgumentException} if the requested job is not found
     */
    public JobStatus getJobStatus(long jobId) {
        if (!isMaster()) {
            throw new JetException("Job status cannot be queried here. Master address: "
                    + nodeEngine.getClusterService().getMasterAddress());
        }

        MasterContext currentMasterContext = masterContexts.get(jobId);
        if (currentMasterContext != null) {
            return currentMasterContext.jobStatus();
        }

        JobRecord jobRecord = jobRepository.getJob(jobId);
        if (jobRecord == null) {
            JobResult jobResult = jobResults.get(jobId);
            if (jobResult != null) {
                return jobResult.getJobStatus();
            }

            throw new IllegalArgumentException("Job " + idToString(jobId) + " not found");
        } else {
            return NOT_STARTED;
        }
    }

    /**
     * Completes the job which is coordinated with the given master context object.
     */
    void completeJob(MasterContext masterContext, long executionId, long completionTime, Throwable error) {
        // the order of operations is important.

        long jobId = masterContext.getJobId();
        JobRecord jobRecord = jobRepository.getJob(jobId);
        if (jobRecord == null) {
            throw new IllegalArgumentException("Job record not found for job id: " + idToString(jobId));
        }

        String coordinator = nodeEngine.getNode().getThisUuid();
        JobResult jobResult = new JobResult(jobId, coordinator, jobRecord.getCreationTime(), completionTime, error);

        JobResult prev = jobResults.putIfAbsent(jobId, jobResult);
        if (prev != null) {
            throw new IllegalStateException(jobResult + " already exists in the " + JOB_RESULTS_MAP_NAME + " map");
        }

        jobRepository.deleteJob(jobId);

        if (masterContexts.remove(masterContext.getJobId(), masterContext)) {
            logger.fine(jobAndExecutionId(jobId, executionId) + " is completed");
        } else {
            MasterContext existing = masterContexts.get(jobId);
            if (existing != null) {
                logger.severe("Different master context found to complete " + jobAndExecutionId(jobId, executionId)
                        + ", master context execution " + idToString(existing.getExecutionId()));
            } else {
                logger.severe("No master context found to complete " + jobAndExecutionId(jobId, executionId));
            }
        }
    }

    /**
     * Schedules a restart task that will be run in future for the given job
     */
    void scheduleRestart(long jobId) {
        MasterContext masterContext = masterContexts.get(jobId);
        if (masterContext != null) {
            logger.fine("Scheduling restart on master for job " + idToString(jobId));
            nodeEngine.getExecutionService().schedule(COORDINATOR_EXECUTOR_NAME, () -> restartJob(jobId),
                    RETRY_DELAY_IN_MILLIS, MILLISECONDS);
        } else {
            logger.severe("Master context for job " + idToString(jobId) + " not found to schedule restart");
        }
    }

    void scheduleSnapshot(long jobId, long executionId) {
        MasterContext masterContext = masterContexts.get(jobId);
        if (masterContext != null) {
            long snapshotInterval = masterContext.getJobConfig().getSnapshotInterval();
            InternalExecutionService executionService = nodeEngine.getExecutionService();
            if (logger.isFineEnabled()) {
                logger.fine(jobAndExecutionId(jobId, executionId) + " snapshot is scheduled in "
                        + snapshotInterval + "ms");
            }
            executionService.schedule(COORDINATOR_EXECUTOR_NAME, () -> this.beginSnapshot(jobId, executionId),
                    snapshotInterval, MILLISECONDS);
        } else {
            logger.warning("MasterContext not found to schedule snapshot of " + jobAndExecutionId(jobId, executionId));
        }
    }

    private void beginSnapshot(long jobId, long executionId) {
        MasterContext masterContext = masterContexts.get(jobId);
        if (masterContext != null) {
            if (masterContext.completionFuture().isDone() || masterContext.jobStatus() != RUNNING) {
                logger.warning("Not starting snapshot since " + jobAndExecutionId(jobId, executionId) + " is done.");
                return;
            }

            if (!shouldStartJobs()) {
                scheduleSnapshot(jobId, executionId);
            }

            masterContext.beginSnapshot(executionId);
        } else {
            logger.warning("MasterContext not found to schedule snapshot of " + jobAndExecutionId(jobId, executionId));
        }
    }

    void completeSnapshot(long jobId, long executionId, long snapshotId, boolean success) {
        MasterContext masterContext = masterContexts.get(jobId);
        if (masterContext != null) {
            try {
                SnapshotStatus status = success ? VALIDATION_NEEDED : FAILED;
                long elapsed = snapshotRepository.setSnapshotStatus(jobId, snapshotId, status);
                logger.info(String.format("Snapshot %s for job %s completed in %dms", snapshotId,
                        idToString(jobId), elapsed));
            } catch (Exception e) {
                logger.warning("Cannot update snapshot status for " + jobAndExecutionId(jobId, executionId) + " snapshot "
                        + snapshotId + " success: " + success);
                return;
            }
            if (success) {
                snapshotRepository.setSnapshotStatus(jobId, snapshotId, SUCCESSFUL);
                snapshotRepository.deleteSnapshots(jobId, snapshotId);
            }
            scheduleSnapshot(jobId, executionId);
        } else {
            logger.warning("MasterContext not found for finalize snapshot of " + jobAndExecutionId(jobId, executionId)
                    + " with result: " + success);
        }
    }

    /**
     * Restarts a job for a new execution if the cluster is stable.
     * Otherwise, it reschedules the restart task.
     */
    private void restartJob(long jobId) {
        MasterContext masterContext = masterContexts.get(jobId);
        if (masterContext != null) {
            if (masterContext.isCancelled()) {
                tryStartJob(masterContext);
                return;
            }

            if (!shouldStartJobs()) {
                scheduleRestart(jobId);
                return;
            }

            tryStartJob(masterContext);
        } else {
            logger.severe("Master context for job " + idToString(jobId) + " not found to restart");
        }
    }

    // runs periodically to restart jobs on coordinator failure and perform gc
    private void scanJobs() {
        if (!shouldStartJobs()) {
            return;
        }

        try {
            Collection<JobRecord> jobs = jobRepository.getJobRecords();
            jobs.forEach(this::startJobIfNotStartedOrCompleted);

            performCleanup();
        } catch (Exception e) {
            if (e instanceof HazelcastInstanceNotActiveException) {
                return;
            }

            logger.severe("Scanning jobs failed", e);
        }
    }

    private boolean shouldStartJobs() {
        if (!(isMaster() && nodeEngine.isRunning())) {
            return false;
        }

        Node node = nodeEngine.getNode();
        InternalPartitionServiceImpl partitionService = (InternalPartitionServiceImpl) node.getPartitionService();
        return partitionService.getPartitionStateManager().isInitialized()
                && partitionService.isMigrationAllowed()
                && !partitionService.hasOnGoingMigrationLocal();
    }

    private void performCleanup() {
        // order is important
        Set<Long> runningJobIds = masterContexts.keySet();
        Set<Long> completedJobIds = jobResults.keySet();
        jobRepository.cleanup(completedJobIds, runningJobIds);
    }

    private boolean isMaster() {
        return nodeEngine.getClusterService().isMaster();
    }

    public SnapshotRepository snapshotRepository() {
        return snapshotRepository;
    }

}
