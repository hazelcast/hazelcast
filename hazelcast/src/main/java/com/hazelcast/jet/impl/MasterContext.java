/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.internal.cluster.MemberInfo;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.impl.execution.init.ExecutionPlan;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.Operation;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.Util.idToString;
import static com.hazelcast.jet.core.JobStatus.NOT_RUNNING;
import static com.hazelcast.jet.core.JobStatus.SUSPENDED;
import static com.hazelcast.jet.impl.util.ExceptionUtil.peel;
import static com.hazelcast.jet.impl.util.ExceptionUtil.withTryCatch;
import static com.hazelcast.jet.impl.util.Util.jobNameAndExecutionId;

/**
 * Data pertaining to single job on master member. There's one instance per job,
 * shared between multiple executions. It has 2 subcomponents:<ul>
 *      <li>{@link MasterJobContext}
 *      <li>{@link MasterSnapshotContext}
 * </ul>
 */
public class MasterContext {

    private static final Object NULL_OBJECT = new Object() {
        @Override
        public String toString() {
            return "NULL_OBJECT";
        }
    };

    private final ReentrantLock lock = new ReentrantLock();

    private final NodeEngineImpl nodeEngine;
    private final JobCoordinationService coordinationService;
    private final ILogger logger;
    private final long jobId;
    private final String jobName;
    private final JobRepository jobRepository;
    private final JobRecord jobRecord;
    private final JobExecutionRecord jobExecutionRecord;
    private volatile JobStatus jobStatus = NOT_RUNNING;
    private volatile long executionId;
    private volatile Map<MemberInfo, ExecutionPlan> executionPlanMap;

    private final MasterJobContext jobContext;
    private final MasterSnapshotContext snapshotContext;

    MasterContext(NodeEngineImpl nodeEngine, JobCoordinationService coordinationService, @Nonnull JobRecord jobRecord,
                  @Nonnull JobExecutionRecord jobExecutionRecord) {
        this.nodeEngine = nodeEngine;
        this.coordinationService = coordinationService;
        this.jobRepository = coordinationService.jobRepository();
        this.logger = nodeEngine.getLogger(getClass());
        this.jobRecord = jobRecord;
        this.jobExecutionRecord = jobExecutionRecord;
        this.jobId = jobRecord.getJobId();
        this.jobName = jobRecord.getJobNameOrId();
        if (jobExecutionRecord.isSuspended()) {
            jobStatus = SUSPENDED;
        }

        jobContext = new MasterJobContext(this, nodeEngine.getLogger(MasterJobContext.class));
        snapshotContext = createMasterSnapshotContext(nodeEngine);
    }

    MasterSnapshotContext createMasterSnapshotContext(NodeEngineImpl nodeEngine) {
        return new MasterSnapshotContext(this, nodeEngine.getLogger(MasterSnapshotContext.class));
    }

    void lock() {
        assertLockNotHeld();
        lock.lock();
    }

    void unlock() {
        lock.unlock();
    }

    void assertLockHeld() {
        assert lock.isHeldByCurrentThread() : "the lock should be held at this place";
    }

    private void assertLockNotHeld() {
        assert !lock.isHeldByCurrentThread() : "the lock should not be held at this place";
    }

    public long jobId() {
        return jobId;
    }

    public long executionId() {
        return executionId;
    }

    public void setExecutionId(long newExecutionId) {
        executionId = newExecutionId;
    }

    public JobStatus jobStatus() {
        return jobStatus;
    }

    void setJobStatus(JobStatus jobStatus) {
        this.jobStatus = jobStatus;
    }

    public JobConfig jobConfig() {
        return jobRecord.getConfig();
    }

    public JobRecord jobRecord() {
        return jobRecord;
    }

    public MasterJobContext jobContext() {
        return jobContext;
    }

    public MasterSnapshotContext snapshotContext() {
        return snapshotContext;
    }

    public JobExecutionRecord jobExecutionRecord() {
        return jobExecutionRecord;
    }

    String jobName() {
        return jobName;
    }

    String jobIdString() {
        return jobNameAndExecutionId(jobName, executionId);
    }

    public JetService getJetService() {
        return coordinationService.getJetService();
    }

    public NodeEngine nodeEngine() {
        return nodeEngine;
    }

    public JobRepository jobRepository() {
        return jobRepository;
    }

    JobCoordinationService coordinationService() {
        return coordinationService;
    }

    Map<MemberInfo, ExecutionPlan> executionPlanMap() {
        return executionPlanMap;
    }

    void setExecutionPlanMap(Map<MemberInfo, ExecutionPlan> executionPlans) {
        executionPlanMap = executionPlans;
    }

    void updateQuorumSize(int newQuorumSize) {
        coordinationService().assertOnCoordinatorThread();
        // This method can be called in parallel if multiple members are added. We don't synchronize here,
        // but the worst that can happen is that we write the JobRecord out unnecessarily.
        if (jobExecutionRecord.getQuorumSize() < newQuorumSize) {
            jobExecutionRecord.setLargerQuorumSize(newQuorumSize);
            writeJobExecutionRecord(false);
            logger.info("Current quorum size: " + jobExecutionRecord.getQuorumSize() + " of job "
                    + idToString(jobRecord.getJobId()) + " is updated to: " + newQuorumSize);
        }
    }

    void writeJobExecutionRecord(boolean canCreate) {
        coordinationService.assertOnCoordinatorThread();
        try {
            coordinationService.jobRepository().writeJobExecutionRecord(jobRecord.getJobId(), jobExecutionRecord,
                    canCreate);
        } catch (RuntimeException e) {
            // We don't bubble up the exceptions, if we can't write the record out, the universe is
            // probably crumbling apart anyway. And we don't depend on it, we only write out for
            // others to know or for the case should the master fail.
            logger.warning("Failed to update JobExecutionRecord", e);
        }
    }

    /**
     * @param completionCallback      a consumer that will receive a collection
     *                                of member-response pairs, one for each
     *                                member, after all have been received. The
     *                                response value will be either the response
     *                                (including a null response) or an
     *                                exception thrown from the operation (the
     *                                pairs themselves will never be null); size
     *                                will be equal to participant count
     * @param errorCallback           A callback that will be called after each
     *                                failure of each individual operation
     * @param retryOnTimeoutException if true, operations that threw {@link
     *                                com.hazelcast.core.OperationTimeoutException}
     *                                will be retried
     */
    void invokeOnParticipants(
            Function<ExecutionPlan, Operation> operationCtor,
            @Nullable Consumer<Collection<Map.Entry<MemberInfo, Object>>> completionCallback,
            @Nullable Consumer<Throwable> errorCallback,
            boolean retryOnTimeoutException
    ) {
        ConcurrentMap<MemberInfo, Object> responses = new ConcurrentHashMap<>();
        AtomicInteger remainingCount = new AtomicInteger(executionPlanMap.size());
        for (Entry<MemberInfo, ExecutionPlan> entry : executionPlanMap.entrySet()) {
            MemberInfo memberInfo = entry.getKey();
            Supplier<Operation> opSupplier = () -> operationCtor.apply(entry.getValue());
            invokeOnParticipant(memberInfo, opSupplier, completionCallback, errorCallback, retryOnTimeoutException,
                    responses, remainingCount);
        }
    }

    private void invokeOnParticipant(
            MemberInfo memberInfo,
            Supplier<Operation> operationSupplier,
            @Nullable Consumer<Collection<Map.Entry<MemberInfo, Object>>> completionCallback,
            @Nullable Consumer<Throwable> errorCallback,
            boolean retryOnTimeoutException,
            ConcurrentMap<MemberInfo, Object> collectedResponses,
            AtomicInteger remainingCount
    ) {
        Operation operation = operationSupplier.get();
        InternalCompletableFuture<Object> future = nodeEngine.getOperationService()
                .createInvocationBuilder(JetService.SERVICE_NAME, operation, memberInfo.getAddress())
                .invoke();

        future.whenCompleteAsync(withTryCatch(logger, (r, throwable) -> {
            Object response = r != null ? r : throwable != null ? peel(throwable) : NULL_OBJECT;
            if (retryOnTimeoutException && throwable instanceof OperationTimeoutException) {
                logger.warning("Retrying " + operation.getClass().getName() + " that failed with "
                        + OperationTimeoutException.class.getSimpleName() + " in " + jobIdString());
                invokeOnParticipant(memberInfo, operationSupplier, completionCallback, errorCallback,
                        retryOnTimeoutException, collectedResponses, remainingCount);
                return;
            }
            if (errorCallback != null && throwable != null) {
                errorCallback.accept(throwable);
            }
            Object oldResponse = collectedResponses.put(memberInfo, response);
            assert oldResponse == null :
                    "Duplicate response for " + memberInfo.getAddress() + ". Old=" + oldResponse + ", new=" + response;
            if (remainingCount.decrementAndGet() == 0 && completionCallback != null) {
                completionCallback.accept(collectedResponses.entrySet().stream()
                        .map(e -> e.getValue() == NULL_OBJECT ? entry(e.getKey(), null) : e)
                        .collect(Collectors.toList()));
            }
        }));
    }
}
