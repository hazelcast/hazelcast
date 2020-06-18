/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.util.counters.Counter;
import com.hazelcast.internal.util.counters.MwCounter;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.JobAlreadyExistsException;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JobNotFoundException;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.core.TopologyChangedException;
import com.hazelcast.jet.core.metrics.MetricNames;
import com.hazelcast.jet.core.metrics.MetricTags;
import com.hazelcast.jet.impl.exception.EnteringPassiveClusterStateException;
import com.hazelcast.jet.impl.execution.DoneItem;
import com.hazelcast.jet.impl.metrics.RawJobMetrics;
import com.hazelcast.jet.impl.observer.ObservableImpl;
import com.hazelcast.jet.impl.observer.WrappedThrowable;
import com.hazelcast.jet.impl.operation.NotifyMemberShutdownOperation;
import com.hazelcast.jet.impl.util.LoggingUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.ringbuffer.OverflowPolicy;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.spi.properties.HazelcastProperties;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.Spliterators;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.hazelcast.cluster.ClusterState.IN_TRANSITION;
import static com.hazelcast.cluster.ClusterState.PASSIVE;
import static com.hazelcast.cluster.memberselector.MemberSelectors.DATA_MEMBER_SELECTOR;
import static com.hazelcast.internal.util.executor.ExecutorType.CACHED;
import static com.hazelcast.jet.Util.idToString;
import static com.hazelcast.jet.core.JetProperties.JOB_SCAN_PERIOD;
import static com.hazelcast.jet.core.JobStatus.COMPLETING;
import static com.hazelcast.jet.core.JobStatus.NOT_RUNNING;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.core.JobStatus.SUSPENDED;
import static com.hazelcast.jet.impl.TerminationMode.CANCEL_FORCEFUL;
import static com.hazelcast.jet.impl.execution.init.CustomClassLoadedObject.deserializeWithCustomClassLoader;
import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.jet.impl.util.ExceptionUtil.withTryCatch;
import static com.hazelcast.jet.impl.util.LoggingUtil.logFine;
import static com.hazelcast.jet.impl.util.LoggingUtil.logFinest;
import static com.hazelcast.jet.impl.util.Util.getJetInstance;
import static java.util.Collections.emptyList;
import static java.util.Comparator.comparing;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;

/**
 * A service that handles MasterContexts on the coordinator member.
 * Job-control operations from client are handled here.
 */
public class JobCoordinationService {

    private static final String COORDINATOR_EXECUTOR_NAME = "jet:coordinator";

    /**
     * The delay before retrying to start/scale up a job.
     */
    private static final long RETRY_DELAY_IN_MILLIS = SECONDS.toMillis(2);
    private static final ThreadLocal<Boolean> IS_JOB_COORDINATOR_THREAD = ThreadLocal.withInitial(() -> false);
    private static final int COORDINATOR_THREADS_POOL_SIZE = 4;

    private static final int MIN_JOB_SCAN_PERIOD_MILLIS = 100;

    private final NodeEngineImpl nodeEngine;
    private final JetService jetService;
    private final JetConfig config;
    private final ILogger logger;
    private final JobRepository jobRepository;
    private final ConcurrentMap<Long, MasterContext> masterContexts = new ConcurrentHashMap<>();
    private final ConcurrentMap<UUID, CompletableFuture<Void>> membersShuttingDown = new ConcurrentHashMap<>();
    /**
     * Map of {memberUuid; removeTime}.
     *
     * A collection of UUIDs of members which left the cluster and for which we
     * didn't receive {@link NotifyMemberShutdownOperation}.
     */
    private final Map<UUID, Long> removedMembers = new ConcurrentHashMap<>();
    private final Object lock = new Object();
    private volatile boolean isClusterEnteringPassiveState;
    private volatile boolean jobsScanned;

    private final AtomicInteger scaleUpScheduledCount = new AtomicInteger();

    @Probe(name = MetricNames.JOBS_SUBMITTED)
    private final Counter jobSubmitted = MwCounter.newMwCounter();
    @Probe(name = MetricNames.JOBS_COMPLETED_SUCCESSFULLY)
    private final Counter jobCompletedSuccessfully = MwCounter.newMwCounter();
    @Probe(name = MetricNames.JOBS_COMPLETED_WITH_FAILURE)
    private final Counter jobCompletedWithFailure = MwCounter.newMwCounter();

    private long maxJobScanPeriodInMillis;

    JobCoordinationService(
            NodeEngineImpl nodeEngine, JetService jetService, JetConfig config, JobRepository jobRepository
    ) {
        this.nodeEngine = nodeEngine;
        this.jetService = jetService;
        this.config = config;
        this.logger = nodeEngine.getLogger(getClass());
        this.jobRepository = jobRepository;

        ExecutionService executionService = nodeEngine.getExecutionService();
        executionService.register(COORDINATOR_EXECUTOR_NAME, COORDINATOR_THREADS_POOL_SIZE, Integer.MAX_VALUE, CACHED);

        // register metrics
        MetricsRegistry registry = nodeEngine.getMetricsRegistry();
        MetricDescriptor descriptor = registry.newMetricDescriptor()
                .withTag(MetricTags.MODULE, "jet");
        registry.registerStaticMetrics(descriptor, this);
    }

    public JobRepository jobRepository() {
        return jobRepository;
    }

    void startScanningForJobs() {
        ExecutionService executionService = nodeEngine.getExecutionService();
        HazelcastProperties properties = new HazelcastProperties(config.getProperties());
        maxJobScanPeriodInMillis = properties.getMillis(JOB_SCAN_PERIOD);
        executionService.schedule(COORDINATOR_EXECUTOR_NAME, this::scanJobs, 0, MILLISECONDS);
    }

    public CompletableFuture<Void> submitJob(long jobId, Data serializedDag, Data serializedConfig) {
        CompletableFuture<Void> res = new CompletableFuture<>();
        submitToCoordinatorThread(() -> {
            MasterContext masterContext;
            try {
                JobConfig config = nodeEngine.getSerializationService().toObject(serializedConfig);
                assertIsMaster("Cannot submit job " + idToString(jobId) + " to non-master node");
                checkOperationalState();

                // the order of operations is important.

                // first, check if the job is already completed
                JobResult jobResult = jobRepository.getJobResult(jobId);
                if (jobResult != null) {
                    logger.fine("Not starting job " + idToString(jobId) + " since already completed with result: "
                            + jobResult);
                    return;
                }

                int quorumSize = config.isSplitBrainProtectionEnabled() ? getQuorumSize() : 0;
                DAG dag = deserializeDag(jobId, config, serializedDag);
                Set<String> ownedObservables = ownedObservables(dag);
                JobRecord jobRecord = new JobRecord(jobId, serializedDag, dagToJson(dag), config, ownedObservables);
                JobExecutionRecord jobExecutionRecord = new JobExecutionRecord(jobId, quorumSize, false);
                masterContext = createMasterContext(jobRecord, jobExecutionRecord);

                boolean hasDuplicateJobName;
                synchronized (lock) {
                    assertIsMaster("Cannot submit job " + idToString(jobId) + " to non-master node");
                    checkOperationalState();
                    hasDuplicateJobName = config.getName() != null && hasActiveJobWithName(config.getName());
                    if (!hasDuplicateJobName) {
                        // just try to initiate the coordination
                        MasterContext prev = masterContexts.putIfAbsent(jobId, masterContext);
                        if (prev != null) {
                            logger.fine("Joining to already existing masterContext " + prev.jobIdString());
                            return;
                        }
                    }
                }

                if (hasDuplicateJobName) {
                    jobRepository.deleteJob(jobId);
                    throw new JobAlreadyExistsException("Another active job with equal name (" + config.getName()
                            + ") exists: " + idToString(jobId));
                }

                // If job is not currently running, it might be that it is just completed
                if (completeMasterContextIfJobAlreadyCompleted(masterContext)) {
                    return;
                }

                // If there is no master context and job result at the same time, it means this is the first submission
                jobSubmitted.inc();
                jobRepository.putNewJobRecord(jobRecord);

                logger.info("Starting job " + idToString(masterContext.jobId()) + " based on submit request");
            } catch (Throwable e) {
                res.completeExceptionally(e);
                throw e;
            } finally {
                res.complete(null);
            }
            tryStartJob(masterContext);
        });
        return res;
    }

    private static Set<String> ownedObservables(DAG dag) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(dag.iterator(), 0), false)
                .map(vertex -> vertex.getMetaSupplier().getTags().get(ObservableImpl.OWNED_OBSERVABLE))
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());
    }

    @SuppressWarnings("WeakerAccess") // used by jet-enterprise
    MasterContext createMasterContext(JobRecord jobRecord, JobExecutionRecord jobExecutionRecord) {
        return new MasterContext(nodeEngine, this, jobRecord, jobExecutionRecord);
    }

    private boolean hasActiveJobWithName(@Nonnull String jobName) {
        // if scanJob() has not run yet, master context objects may not be initialized.
        // in this case, we cannot check if the new job submission has a duplicate job name.
        // therefore, we will retry until scanJob() task runs at least once.
        if (!jobsScanned) {
            throw new RetryableHazelcastException("Cannot submit job with name '" + jobName
                    + "' before the master node initializes job coordination service's state");
        }

        return masterContexts.values()
                             .stream()
                             .anyMatch(ctx -> jobName.equals(ctx.jobConfig().getName()));
    }

    public CompletableFuture<Void> prepareForPassiveClusterState() {
        assertIsMaster("Cannot prepare for passive cluster state on a non-master node");
        synchronized (lock) {
            isClusterEnteringPassiveState = true;
        }
        return submitToCoordinatorThread(() -> {
            CompletableFuture[] futures = masterContexts
                    .values().stream()
                    .map(mc -> mc.jobContext().gracefullyTerminate())
                    .toArray(CompletableFuture[]::new);
            return CompletableFuture.allOf(futures);
        }).thenCompose(identity());
    }

    void clusterChangeDone() {
        synchronized (lock) {
            isClusterEnteringPassiveState = false;
        }
    }

    public void reset() {
        assert !isMaster() : "this member is a master";
        List<MasterContext> contexts;
        synchronized (lock) {
            contexts = new ArrayList<>(masterContexts.values());
            masterContexts.clear();
            jobsScanned = false;
        }

        contexts.forEach(ctx -> ctx.jobContext().setFinalResult(new CancellationException()));
    }

    public CompletableFuture<Void> joinSubmittedJob(long jobId) {
        checkOperationalState();
        CompletableFuture<CompletableFuture<Void>> future = callWithJob(jobId,
                mc -> mc.jobContext().jobCompletionFuture()
                        .handle((r, t) -> {
                            if (t == null) {
                                return null;
                            }
                            if (t instanceof CancellationException || t instanceof JetException) {
                                throw sneakyThrow(t);
                            }
                            throw new JetException(t.toString(), t);
                        }),
                JobResult::asCompletableFuture,
                jobRecord -> {
                    JobExecutionRecord jobExecutionRecord = ensureExecutionRecord(jobId,
                            jobRepository.getJobExecutionRecord(jobId));
                    return startJobIfNotStartedOrCompleted(jobRecord, jobExecutionRecord, "join request from client");
                },
                null
        );

        return future
                .thenCompose(identity()); // unwrap the inner future
    }

    public CompletableFuture<Void> terminateJob(long jobId, TerminationMode terminationMode) {
        return runWithJob(jobId,
                masterContext -> {
                    // User can cancel in any state, other terminations are allowed only when running.
                    // This is not technically required (we can request termination in any state),
                    // but this method is only called by the user. It would be weird for the client to
                    // request a restart if the job didn't start yet etc.
                    // Also, it would be weird to restart the job during STARTING: as soon as it will start,
                    // it will restart.
                    // In any case, it doesn't make sense to restart a suspended job.
                    JobStatus jobStatus = masterContext.jobStatus();
                    if (jobStatus != RUNNING && terminationMode != CANCEL_FORCEFUL) {
                        throw new IllegalStateException("Cannot " + terminationMode + ", job status is " + jobStatus
                                + ", should be " + RUNNING);
                    }

                    String terminationResult = masterContext.jobContext().requestTermination(terminationMode, false).f1();
                    if (terminationResult != null) {
                        throw new IllegalStateException("Cannot " + terminationMode + ": " + terminationResult);
                    }
                },
                jobResult -> {
                    if (terminationMode != CANCEL_FORCEFUL) {
                        throw new IllegalStateException("Cannot " + terminationMode + " job " + idToString(jobId)
                                + " because it already has a result: " + jobResult);
                    }
                    logger.fine("Ignoring cancellation of a completed job " + idToString(jobId));
                },
                jobExecutionRecord -> {
                    // we'll eventually learn of the job through scanning of records or from a join operation
                    throw new RetryableHazelcastException("No MasterContext found for job " + idToString(jobId) + " for "
                            + terminationMode);
                }
        );
    }

    public CompletableFuture<List<Long>> getAllJobIds() {
        assertIsMaster("Cannot query list of job ids on non-master node");

        return submitToCoordinatorThread(() -> {
            Set<Long> jobIds = new HashSet<>(jobRepository.getAllJobIds());
            jobIds.addAll(masterContexts.keySet());
            return new ArrayList<>(jobIds);
        });
    }

    /**
     * Return the job IDs of jobs with the given name, sorted by
     * {active/completed, creation time}, active & newest first.
     */
    public CompletableFuture<List<Long>> getJobIds(@Nonnull String name) {
        assertIsMaster("Cannot query list of job ids on non-master node");

        return submitToCoordinatorThread(() -> {
            Map<Long, Long> jobs = new HashMap<>();
            for (MasterContext ctx : masterContexts.values()) {
                if (name.equals(ctx.jobConfig().getName())) {
                    jobs.put(ctx.jobId(), Long.MAX_VALUE);
                }
            }

            jobRepository.getJobResults(name)
                         .forEach(jobResult -> jobs.put(jobResult.getJobId(), jobResult.getCreationTime()));

            return jobs.entrySet().stream()
                       .sorted(comparing(Entry<Long, Long>::getValue).reversed())
                       .map(Entry::getKey)
                       .collect(toList());
        });
    }

    /**
     * Returns the job status or fails with {@link JobNotFoundException}
     * if the requested job is not found.
     */
    public CompletableFuture<JobStatus> getJobStatus(long jobId) {
        return callWithJob(jobId,
                mc -> {
                    JobStatus jobStatus = mc.jobStatus();
                    return jobStatus == RUNNING && mc.jobContext().requestedTerminationMode() != null
                            ? COMPLETING
                            : jobStatus;
                },
                JobResult::getJobStatus,
                null,
                jobExecutionRecord -> jobExecutionRecord.isSuspended() ? SUSPENDED : NOT_RUNNING
        );
    }

    /**
     * Returns the latest metrics for a job or fails with {@link JobNotFoundException}
     * if the requested job is not found.
     */
    public CompletableFuture<List<RawJobMetrics>> getJobMetrics(long jobId) {
        CompletableFuture<List<RawJobMetrics>> cf = new CompletableFuture<>();
        runWithJob(jobId,
                mc -> mc.jobContext().collectMetrics(cf),
                jobResult -> {
                    List<RawJobMetrics> metrics = jobRepository.getJobMetrics(jobId);
                    cf.complete(metrics != null ? metrics : emptyList());
                },
                record -> cf.complete(emptyList())
        );
        return cf;
    }

    /**
     * Returns the job submission time or fails with {@link JobNotFoundException}
     * if the requested job is not found.
     */
    public CompletableFuture<Long> getJobSubmissionTime(long jobId) {
        return callWithJob(jobId,
                mc -> mc.jobRecord().getCreationTime(),
                JobResult::getCreationTime,
                JobRecord::getCreationTime,
                null
        );
    }

    public CompletableFuture<Void> resumeJob(long jobId) {
        return runWithJob(jobId,
                masterContext -> masterContext.jobContext().resumeJob(jobRepository::newExecutionId),
                jobResult -> {
                    throw new IllegalStateException("Job already completed");
                },
                jobExecutionRecord -> {
                    throw new RetryableHazelcastException("Job " + idToString(jobId) + " not yet discovered");
                }
        );
    }

    /**
     * Return a summary of all jobs
     */
    public CompletableFuture<List<JobSummary>> getJobSummaryList() {
        return submitToCoordinatorThread(() -> {
            Map<Long, JobSummary> jobs = new HashMap<>();

            // running jobs
            jobRepository.getJobRecords().stream().map(this::getJobSummary).forEach(s -> jobs.put(s.getJobId(), s));

            // completed jobs
            jobRepository.getJobResults().stream()
                         .map(r -> new JobSummary(
                                 r.getJobId(), r.getJobNameOrId(), r.getJobStatus(), r.getCreationTime(),
                                 r.getCompletionTime(), r.getFailureText())
                         ).forEach(s -> jobs.put(s.getJobId(), s));

            return jobs.values().stream().sorted(comparing(JobSummary::getSubmissionTime).reversed()).collect(toList());
        });
    }

    /**
     * Add the given member to shutting down members. This will prevent
     * submission of more executions until the member actually leaves the
     * cluster. The returned future will complete when all executions of which
     * the member is a participant terminate.
     * <p>
     * The method is idempotent, the {@link NotifyMemberShutdownOperation}
     * which calls it can be retried.
     */
    @Nonnull
    public CompletableFuture<Void> addShuttingDownMember(UUID uuid) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        CompletableFuture<Void> oldFuture = membersShuttingDown.putIfAbsent(uuid, future);
        if (oldFuture != null) {
            return oldFuture;
        }
        if (removedMembers.containsKey(uuid)) {
            logFine(logger, "NotifyMemberShutdownOperation received for a member that was already " +
                    "removed from the cluster: %s", uuid);
            return CompletableFuture.completedFuture(null);
        }
        logFine(logger, "Added a shutting-down member: %s", uuid);
        CompletableFuture[] futures = masterContexts.values().stream()
                                                    .map(mc -> mc.jobContext().onParticipantGracefulShutdown(uuid))
                                                    .toArray(CompletableFuture[]::new);
        // Need to do this even if futures.length == 0, we need to perform the action in whenComplete
        CompletableFuture.allOf(futures)
                         .whenComplete(withTryCatch(logger, (r, e) -> future.complete(null)));
        return future;
    }

    // only for testing
    public Map<Long, MasterContext> getMasterContexts() {
        return new HashMap<>(masterContexts);
    }

    // only for testing
    public MasterContext getMasterContext(long jobId) {
        return masterContexts.get(jobId);
    }

    JetService getJetService() {
        return jetService;
    }

    boolean shouldStartJobs() {
        if (!isMaster() || !nodeEngine.isRunning()) {
            return false;
        }

        ClusterState clusterState = nodeEngine.getClusterService().getClusterState();
        if (isClusterEnteringPassiveState || clusterState == PASSIVE || clusterState == IN_TRANSITION) {
            logger.fine("Not starting jobs because cluster is in passive state or in transition.");
            return false;
        }
        // if there are any members in a shutdown process, don't start jobs
        if (!membersShuttingDown.isEmpty()) {
            LoggingUtil.logFine(logger, "Not starting jobs because members are shutting down: %s",
                    membersShuttingDown.keySet());
            return false;
        }
        if (!getInternalPartitionService().isMemberStateSafe()) {
            logger.fine("Not starting jobs because master is not in safe state.");
            return false;
        }
        if (!getInternalPartitionService().getPartitionStateManager().isInitialized()) {
            logger.fine("Not starting jobs because partitions are not yet initialized.");
            return false;
        }
        return true;
    }

    private CompletableFuture<Void> runWithJob(
            long jobId,
            @Nonnull Consumer<MasterContext> masterContextHandler,
            @Nonnull Consumer<JobResult> jobResultHandler,
            @Nullable Consumer<JobExecutionRecord> jobExecutionRecordHandler
    ) {
        return callWithJob(jobId,
                toNullFunction(masterContextHandler),
                toNullFunction(jobResultHandler),
                toNullFunction(null),
                toNullFunction(jobExecutionRecordHandler)
        );
    }

    private <T, R> Function<T, R> toNullFunction(Consumer<T> consumer) {
        return val -> {
            consumer.accept(val);
            return null;
        };
    }

    private <T> CompletableFuture<T> callWithJob(
            long jobId,
            @Nonnull Function<MasterContext, T> masterContextHandler,
            @Nonnull Function<JobResult, T> jobResultHandler,
            @Nullable Function<JobRecord, T> jobRecordHandler,
            @Nullable Function<JobExecutionRecord, T> jobExecutionRecordHandler
    ) {
        assertIsMaster("Cannot do this task on non-master. jobId=" + idToString(jobId));
        if (jobRecordHandler == null && jobExecutionRecordHandler == null) {
            throw new IllegalArgumentException();
        }

        return submitToCoordinatorThread(() -> {
            // when job is finalized, actions happen in this order:
            // - JobResult and JobMetrics are created
            // - JobRecord and JobExecutionRecord are deleted
            // - masterContext is removed from the map
            // We check them in reverse order so that no race is possible.
            //
            // We check the JobResult after MasterContext for optimization because in most cases
            // there will either be MasterContext or JobResult. Neither of them is present only after
            // master failed and the new master didn't yet scan jobs. We check the JobResult
            // again at the end for correctness.

            // check masterContext first
            MasterContext mc = masterContexts.get(jobId);
            if (mc != null) {
                return masterContextHandler.apply(mc);
            }

            // early check of JobResult.
            JobResult jobResult = jobRepository.getJobResult(jobId);
            if (jobResult != null) {
                return jobResultHandler.apply(jobResult);
            }

            // the job might not be yet discovered by job record scanning
            JobExecutionRecord jobExRecord;
            if (jobExecutionRecordHandler != null && (jobExRecord = jobRepository.getJobExecutionRecord(jobId)) != null) {
                return jobExecutionRecordHandler.apply(jobExRecord);
            }
            JobRecord jobRecord;
            if (jobRecordHandler != null && (jobRecord = jobRepository.getJobRecord(jobId)) != null) {
                return jobRecordHandler.apply(jobRecord);
            }

            // second check for JobResult, see comment at the top of the method
            jobResult = jobRepository.getJobResult(jobId);
            if (jobResult != null) {
                return jobResultHandler.apply(jobResult);
            }

            // job doesn't exist
            throw new JobNotFoundException(jobId);
        });
    }

    void onMemberAdded(MemberImpl addedMember) {
        // the member can re-join with the same UUID in certain scenarios
        removedMembers.remove(addedMember.getUuid());
        if (addedMember.isLiteMember()) {
            return;
        }

        updateQuorumValues();
        scheduleScaleUp(config.getInstanceConfig().getScaleUpDelayMillis());
    }

    void onMemberRemoved(UUID uuid) {
        if (membersShuttingDown.remove(uuid) != null) {
            logFine(logger, "Removed a shutting-down member: %s, now shuttingDownMembers=%s",
                    uuid, membersShuttingDown.keySet());
        } else {
            removedMembers.put(uuid, System.nanoTime());
        }

        // clean up old entries from removedMembers (the value is time when the member was removed)
        long removeThreshold = System.nanoTime() - HOURS.toNanos(1);
        removedMembers.entrySet().removeIf(en -> en.getValue() < removeThreshold);
    }

    boolean isQuorumPresent(int quorumSize) {
        return getDataMemberCount() >= quorumSize;
    }

    /**
     * Completes the job which is coordinated with the given master context object.
     */
    @CheckReturnValue
    CompletableFuture<Void> completeJob(MasterContext masterContext, Throwable error) {
        return submitToCoordinatorThread(() -> {
            // the order of operations is important.
            List<RawJobMetrics> jobMetrics =
                    masterContext.jobConfig().isStoreMetricsAfterJobCompletion()
                            ? masterContext.jobContext().jobMetrics()
                            : null;
            jobRepository.completeJob(masterContext, jobMetrics, error);
            if (masterContexts.remove(masterContext.jobId(), masterContext)) {
                completeObservables(masterContext.jobRecord().getOwnedObservables(), error);
                logger.fine(masterContext.jobIdString() + " is completed");
                (error == null ? jobCompletedSuccessfully : jobCompletedWithFailure).inc();
            } else {
                MasterContext existing = masterContexts.get(masterContext.jobId());
                if (existing != null) {
                    logger.severe("Different master context found to complete " + masterContext.jobIdString()
                            + ", master context execution " + idToString(existing.executionId()));
                } else {
                    logger.severe("No master context found to complete " + masterContext.jobIdString());
                }
            }
        });
    }

    /**
     * Schedules a restart task that will be run in future for the given job
     */
    void scheduleRestart(long jobId) {
        MasterContext masterContext = masterContexts.get(jobId);
        if (masterContext == null) {
            logger.severe("Master context for job " + idToString(jobId) + " not found to schedule restart");
            return;
        }
        logger.fine("Scheduling restart on master for job " + masterContext.jobName());
        nodeEngine.getExecutionService().schedule(COORDINATOR_EXECUTOR_NAME, () -> restartJob(jobId),
                RETRY_DELAY_IN_MILLIS, MILLISECONDS);
    }

    void scheduleSnapshot(MasterContext mc, long executionId) {
        long snapshotInterval = mc.jobConfig().getSnapshotIntervalMillis();
        ExecutionService executionService = nodeEngine.getExecutionService();
        if (logger.isFineEnabled()) {
            logger.fine(mc.jobIdString() + " snapshot is scheduled in " + snapshotInterval + "ms");
        }
        executionService.schedule(COORDINATOR_EXECUTOR_NAME,
                () -> mc.snapshotContext().startScheduledSnapshot(executionId),
                snapshotInterval, MILLISECONDS);
    }

    /**
     * Restarts a job for a new execution if the cluster is stable.
     * Otherwise, it reschedules the restart task.
     */
    void restartJob(long jobId) {
        MasterContext masterContext = masterContexts.get(jobId);
        if (masterContext == null) {
            logger.severe("Master context for job " + idToString(jobId) + " not found to restart");
            return;
        }
        tryStartJob(masterContext);
    }

    private void checkOperationalState() {
        if (isClusterEnteringPassiveState) {
            throw new EnteringPassiveClusterStateException();
        }
    }

    private void scheduleScaleUp(long delay) {
        int counter = scaleUpScheduledCount.incrementAndGet();
        nodeEngine.getExecutionService().schedule(() -> scaleJobsUpNow(counter), delay, MILLISECONDS);
    }

    private void scaleJobsUpNow(int counter) {
        // if another scale-up was scheduled after this one, ignore this one
        if (scaleUpScheduledCount.get() != counter) {
            return;
        }
        // if we can't start jobs yet, we also won't tear them down
        if (!shouldStartJobs()) {
            scheduleScaleUp(RETRY_DELAY_IN_MILLIS);
            return;
        }

        submitToCoordinatorThread(() -> {
            boolean allSucceeded = true;
            int dataMembersCount = nodeEngine.getClusterService().getMembers(DATA_MEMBER_SELECTOR).size();
            int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
            // If the number of partitions is lower than the data member count, some members won't have
            // any partitions assigned. Jet doesn't use such members.
            int dataMembersWithPartitionsCount = Math.min(dataMembersCount, partitionCount);
            for (MasterContext mc : masterContexts.values()) {
                allSucceeded &= mc.jobContext().maybeScaleUp(dataMembersWithPartitionsCount);
            }
            if (!allSucceeded) {
                scheduleScaleUp(RETRY_DELAY_IN_MILLIS);
            }
        });
    }

    /**
     * Scans all job records and updates quorum size of a split-brain protection enabled
     * job with current cluster quorum size if the current cluster quorum size is larger
     */
    private void updateQuorumValues() {
        if (!shouldCheckQuorumValues()) {
            return;
        }

        submitToCoordinatorThread(() -> {
            try {
                int currentQuorumSize = getQuorumSize();
                for (JobRecord jobRecord : jobRepository.getJobRecords()) {
                    try {
                        if (!jobRecord.getConfig().isSplitBrainProtectionEnabled()) {
                            continue;
                        }
                        MasterContext masterContext = masterContexts.get(jobRecord.getJobId());
                        // if MasterContext doesn't exist, update in the IMap directly, using a sync method
                        if (masterContext == null) {
                            jobRepository.updateJobQuorumSizeIfSmaller(jobRecord.getJobId(), currentQuorumSize);
                            // check the master context again, it might have been just created and have picked
                            // up the JobRecord before being updated
                            masterContext = masterContexts.get(jobRecord.getJobId());
                        }
                        if (masterContext != null) {
                            masterContext.updateQuorumSize(currentQuorumSize);
                        }
                    } catch (Exception e) {
                        logger.severe("Quorum of job " + idToString(jobRecord.getJobId())
                                + " could not be updated to " + currentQuorumSize, e);
                    }
                }
            } catch (Exception e) {
                logger.severe("update quorum values task failed", e);
            }
        });
    }

    private boolean shouldCheckQuorumValues() {
        return isMaster() && nodeEngine.isRunning()
                && getInternalPartitionService().getPartitionStateManager().isInitialized();
    }

    private DAG deserializeDag(long jobId, JobConfig jobConfig, Data dagData) {
        ClassLoader classLoader = jetService.getJobExecutionService().getClassLoader(jobConfig, jobId);
        return deserializeWithCustomClassLoader(nodeEngine.getSerializationService(), classLoader, dagData);
    }

    private String dagToJson(DAG dag) {
        int coopThreadCount = getJetInstance(nodeEngine).getConfig().getInstanceConfig().getCooperativeThreadCount();
        return dag.toJson(coopThreadCount).toString();
    }

    private CompletableFuture<Void> startJobIfNotStartedOrCompleted(
            @Nonnull JobRecord jobRecord,
            @Nonnull JobExecutionRecord jobExecutionRecord, String reason
    ) {
        // the order of operations is important.
        long jobId = jobRecord.getJobId();
        JobResult jobResult = jobRepository.getJobResult(jobId);
        if (jobResult != null) {
            logger.fine("Not starting job " + idToString(jobId) + ", already has result: " + jobResult);
            return jobResult.asCompletableFuture();
        }

        MasterContext masterContext;
        MasterContext oldMasterContext;
        synchronized (lock) {
            checkOperationalState();

            masterContext = createMasterContext(jobRecord, jobExecutionRecord);
            oldMasterContext = masterContexts.putIfAbsent(jobId, masterContext);
        }

        if (oldMasterContext != null) {
            return oldMasterContext.jobContext().jobCompletionFuture();
        }

        // If job is not currently running, it might be that it just completed.
        // Since we've put the MasterContext into the masterContexts map, someone else could
        // have joined to the job in the meantime so we should notify its future.
        if (completeMasterContextIfJobAlreadyCompleted(masterContext)) {
            return masterContext.jobContext().jobCompletionFuture();
        }

        if (jobExecutionRecord.isSuspended()) {
            logFinest(logger, "MasterContext for suspended %s is created", masterContext.jobIdString());
        } else {
            logger.info("Starting job " + idToString(jobId) + ": " + reason);
            tryStartJob(masterContext);
        }

        return masterContext.jobContext().jobCompletionFuture();
    }

    // If a job result is present, it completes the master context using the job result
    private boolean completeMasterContextIfJobAlreadyCompleted(MasterContext masterContext) {
        long jobId = masterContext.jobId();
        JobResult jobResult = jobRepository.getJobResult(jobId);
        if (jobResult != null) {
            logger.fine("Completing master context for " + masterContext.jobIdString()
                    + " since already completed with result: " + jobResult);
            masterContext.jobContext().setFinalResult(jobResult.getFailureAsThrowable());
            return masterContexts.remove(jobId, masterContext);
        }

        if (!masterContext.jobConfig().isAutoScaling() && masterContext.jobExecutionRecord().executed()) {
            logger.info("Suspending or failing " + masterContext.jobIdString()
                    + " since auto-restart is disabled and the job has been executed before");
            masterContext.jobContext().finalizeJob(new TopologyChangedException());
            return true;
        }

        return false;
    }

    private void tryStartJob(MasterContext masterContext) {
        masterContext.jobContext().tryStartJob(jobRepository::newExecutionId);
    }

    private int getQuorumSize() {
        return (getDataMemberCount() / 2) + 1;
    }

    private int getDataMemberCount() {
        ClusterService clusterService = nodeEngine.getClusterService();
        return clusterService.getMembers(DATA_MEMBER_SELECTOR).size();
    }

    private JobSummary getJobSummary(JobRecord record) {
        MasterContext ctx = masterContexts.get(record.getJobId());
        long execId = ctx == null ? 0 : ctx.executionId();
        JobStatus status;
        if (ctx == null) {
            JobExecutionRecord executionRecord = jobRepository.getJobExecutionRecord(record.getJobId());
            status = executionRecord != null && executionRecord.isSuspended()
                    ? JobStatus.SUSPENDED : JobStatus.NOT_RUNNING;
        } else {
            status = ctx.jobStatus();
        }
        return new JobSummary(record.getJobId(), execId, record.getJobNameOrId(), status, record.getCreationTime());
    }

    private InternalPartitionServiceImpl getInternalPartitionService() {
        Node node = nodeEngine.getNode();
        return (InternalPartitionServiceImpl) node.getPartitionService();
    }

    // runs periodically to restart jobs on coordinator failure and perform GC
    private void scanJobs() {
        long nextScanDelay = maxJobScanPeriodInMillis;
        try {
            // explicit check for master because we don't want to use shorter delay on non-master nodes
            // it will be checked again in shouldStartJobs()
            if (isMaster()) {
                if (shouldStartJobs()) {
                    doScanJobs();
                } else {
                    // use a smaller delay when cluster is not in ready state
                    nextScanDelay = MIN_JOB_SCAN_PERIOD_MILLIS;
                }
            }
        } catch (HazelcastInstanceNotActiveException ignored) {
            // ignore this exception
        } catch (Exception e) {
            logger.severe("Scanning jobs failed", e);
        }
        ExecutionService executionService = nodeEngine.getExecutionService();
        executionService.schedule(this::scanJobs, nextScanDelay, MILLISECONDS);
    }

    private void doScanJobs() {
        Collection<JobRecord> jobs = jobRepository.getJobRecords();
        for (JobRecord jobRecord : jobs) {
            JobExecutionRecord jobExecutionRecord = ensureExecutionRecord(jobRecord.getJobId(),
                    jobRepository.getJobExecutionRecord(jobRecord.getJobId()));
            startJobIfNotStartedOrCompleted(jobRecord, jobExecutionRecord, "discovered by scanning of JobRecords");
        }
        jobRepository.cleanup(nodeEngine);
        if (!jobsScanned) {
            synchronized (lock) {
                jobsScanned = true;
            }
        }
    }

    private JobExecutionRecord ensureExecutionRecord(long jobId, JobExecutionRecord record) {
        return record != null ? record : new JobExecutionRecord(jobId, getQuorumSize(), false);
    }

    @SuppressWarnings("WeakerAccess") // used by jet-enterprise
    void assertIsMaster(String error) {
        if (!isMaster()) {
            throw new JetException(error + ". Master address: " + nodeEngine.getClusterService().getMasterAddress());
        }
    }

    private boolean isMaster() {
        return nodeEngine.getClusterService().isMaster();
    }

    @SuppressWarnings("unused") // used in jet-enterprise
    NodeEngineImpl nodeEngine() {
        return nodeEngine;
    }

    CompletableFuture<Void> submitToCoordinatorThread(Runnable action) {
        return submitToCoordinatorThread(() -> {
            action.run();
            return null;
        });
    }

    <T> CompletableFuture<T> submitToCoordinatorThread(Callable<T> action) {
        // if we are on our thread already, execute directly in a blocking way
        if (IS_JOB_COORDINATOR_THREAD.get()) {
            try {
                return CompletableFuture.completedFuture(action.call());
            } catch (Throwable e) {
                return com.hazelcast.jet.impl.util.Util.exceptionallyCompletedFuture(e);
            }
        }

        Future<T> future = nodeEngine.getExecutionService().submit(COORDINATOR_EXECUTOR_NAME, () -> {
            assert !IS_JOB_COORDINATOR_THREAD.get() : "flag already raised";
            IS_JOB_COORDINATOR_THREAD.set(true);
            try {
                return action.call();
            } catch (Throwable e) {
                // the executor ignores exceptions, let's log it at least
                logger.warning(null, e);
                throw e;
            } finally {
                IS_JOB_COORDINATOR_THREAD.set(false);
            }
        });
        return nodeEngine.getExecutionService().asCompletableFuture(future);
    }

    void assertOnCoordinatorThread() {
        assert IS_JOB_COORDINATOR_THREAD.get() : "not on coordinator thread";
    }

    private void completeObservables(Set<String> observables, Throwable error) {
        for (String observable : observables) {
            String ringbufferName = ObservableImpl.ringbufferName(observable);
            Ringbuffer<Object> ringbuffer = nodeEngine.getHazelcastInstance().getRingbuffer(ringbufferName);
            Object completion = error == null ? DoneItem.DONE_ITEM : WrappedThrowable.of(error);
            ringbuffer.addAsync(completion, OverflowPolicy.OVERWRITE);
        }
    }
}
