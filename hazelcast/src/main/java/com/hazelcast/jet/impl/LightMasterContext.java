/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cluster.Address;
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.internal.cluster.MemberInfo;
import com.hazelcast.internal.cluster.impl.MembersView;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.impl.exception.CancellationByUserException;
import com.hazelcast.jet.impl.exception.JobTerminateRequestedException;
import com.hazelcast.jet.impl.execution.init.ExecutionPlan;
import com.hazelcast.jet.impl.operation.InitExecutionOperation;
import com.hazelcast.jet.impl.operation.TerminateExecutionOperation;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.eventservice.impl.Registration;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.impl.InvocationFuture;
import com.hazelcast.version.Version;

import javax.annotation.Nullable;
import javax.security.auth.Subject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.hazelcast.internal.util.ConcurrencyUtil.CALLER_RUNS;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.jet.Util.idToString;
import static com.hazelcast.jet.core.JobStatus.COMPLETED;
import static com.hazelcast.jet.core.JobStatus.FAILED;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.impl.AbstractJobProxy.cannotAddStatusListener;
import static com.hazelcast.jet.impl.TerminationMode.CANCEL_FORCEFUL;
import static com.hazelcast.jet.impl.execution.init.ExecutionPlanBuilder.createExecutionPlans;
import static com.hazelcast.jet.impl.util.ExceptionUtil.peel;
import static com.hazelcast.jet.impl.util.LoggingUtil.logFine;
import static com.hazelcast.jet.impl.util.Util.doWithClassLoader;
import static com.hazelcast.spi.impl.executionservice.ExecutionService.JOB_OFFLOADABLE_EXECUTOR;
import static java.util.concurrent.CompletableFuture.runAsync;

public final class LightMasterContext {

    private static final Object NULL_OBJECT = new Object() {
        @Override
        public String toString() {
            return "NULL_OBJECT";
        }
    };

    private final ReentrantLock lock = new ReentrantLock();

    private final NodeEngine nodeEngine;
    private final JobEventService jobEventService;
    private final long jobId;

    private final ILogger logger;
    private final String jobIdString;
    private final JobConfig jobConfig;
    private final long startTime = System.currentTimeMillis();

    private final Map<MemberInfo, ExecutionPlan> executionPlanMap;
    private final AtomicBoolean invocationsCancelled = new AtomicBoolean();
    private final CompletableFuture<Void> jobCompletionFuture = new CompletableFuture<>();
    private final Set<Vertex> vertices;
    private final JobClassLoaderService jobClassLoaderService;
    private volatile boolean userInitiatedTermination;

    private LightMasterContext(NodeEngine nodeEngine, long jobId, ILogger logger, String jobIdString,
                               JobConfig jobConfig, Map<MemberInfo, ExecutionPlan> executionPlanMap,
                               Set<Vertex> vertices) {
        this.nodeEngine = nodeEngine;
        this.jobEventService = nodeEngine.getService(JobEventService.SERVICE_NAME);
        this.jobId = jobId;
        this.logger = logger;
        this.jobIdString = jobIdString;
        this.jobConfig = jobConfig;
        this.executionPlanMap = executionPlanMap;
        this.vertices = vertices;
        this.jobClassLoaderService = ((JetServiceBackend) nodeEngine.getService(JetServiceBackend.SERVICE_NAME))
                .getJobClassLoaderService();
    }

    @SuppressWarnings("checkstyle:ExecutableStatementCount")
    public static CompletableFuture<LightMasterContext> createContext(
            NodeEngineImpl nodeEngine, JobCoordinationService coordinationService, DAG dag, long jobId,
            JobConfig jobConfig, Subject subject
    ) {
        ILogger logger = nodeEngine.getLogger(LightMasterContext.class);
        String jobIdString = idToString(jobId);

        // find a subset of members with version equal to the coordinator version.
        MembersView membersView = Util.getMembersView(nodeEngine);
        Version coordinatorVersion = nodeEngine.getLocalMember().getVersion().asVersion();
        List<MemberInfo> members = membersView.getMembers().stream()
                .filter(m -> m.getVersion().asVersion().equals(coordinatorVersion)
                        && !m.isLiteMember()
                        && !coordinationService.isMemberShuttingDown(m.getUuid()))
                .collect(Collectors.toList());
        if (members.isEmpty()) {
            throw new JetException("No data member with version equal to the coordinator version found");
        }
        if (members.size() < membersView.size()) {
            logFine(logger, "Light job %s will run on a subset of members: %d out of %d members with version %s",
                    idToString(jobId), members.size(), membersView.size(), coordinatorVersion);
        }

        if (logger.isFineEnabled()) {
            JetConfig jetConfig = nodeEngine.getConfig().getJetConfig();
            String dotRepresentation = dag.toDotString(jetConfig.getCooperativeThreadCount(),
                    jetConfig.getDefaultEdgeConfig().getQueueSize());
            logFine(logger, "Start executing light job %s, execution graph in DOT format:\n%s"
                            + "\nHINT: You can use graphviz or http://viz-js.com to visualize the printed graph.",
                    jobIdString, dotRepresentation);
            logFine(logger, "Job config for %s: %s", jobIdString, jobConfig);
            logFine(logger, "Building execution plan for %s", jobIdString);
        }

        Set<Vertex> vertices = new HashSet<>();
        dag.iterator().forEachRemaining(vertices::add);
        return createExecutionPlans(nodeEngine, members, dag, jobId, jobId, jobConfig, 0, true, subject)
                .handleAsync((planMap, e) -> {
                    LightMasterContext mc = new LightMasterContext(nodeEngine, jobId, logger,
                            jobIdString, jobConfig, planMap, vertices);
                    if (e != null) {
                        mc.finalizeJob(e);
                        throw rethrow(e);
                    }
                    logFine(logger, "Built execution plans for %s", jobIdString);
                    Set<MemberInfo> participants = planMap.keySet();

                    coordinationService.jobInvocationObservers.forEach(obs ->
                            obs.onLightJobInvocation(jobId, participants, dag, jobConfig));

                    Function<ExecutionPlan, Operation> operationCtor = plan -> {
                        Data serializedPlan = nodeEngine.getSerializationService().toData(plan);
                        return new InitExecutionOperation(jobId, jobId, membersView.getVersion(), coordinatorVersion,
                                participants, serializedPlan, true);
                    };

                    mc.invokeOnParticipants(operationCtor,
                            responses -> mc.finalizeJob(mc.findError(responses)),
                            error -> mc.cancelInvocations()
                    );

                    return mc;
                }, coordinationService.coordinationExecutor());
    }

    public long getJobId() {
        return jobId;
    }

    private void finalizeJob(@Nullable final Throwable failure) {
        ExecutorService offloadExecutor = nodeEngine.getExecutionService().getExecutor(JOB_OFFLOADABLE_EXECUTOR);
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        // close ProcessorMetaSuppliers
        for (Vertex vertex : vertices) {
            ProcessorMetaSupplier metaSupplier = vertex.getMetaSupplier();
            Executor executor = metaSupplier.closeIsCooperative() ? CALLER_RUNS : offloadExecutor;
            futures.add(runAsync(() -> invokeClose(failure, metaSupplier), executor));
        }

        CompletableFuture<Void> combined = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));

        combined.whenComplete((ignored, e) -> {
            lock.lock();
            try {
                Throwable fail = failure;
                if (fail == null) {
                    jobCompletionFuture.complete(null);
                    jobEventService.publishEvent(jobId, RUNNING, COMPLETED, null, false);
                } else {
                    TerminationMode requestedTerminationMode = fail instanceof JobTerminateRequestedException
                            ? ((JobTerminateRequestedException) fail).mode() : null;
                    // translate JobTerminateRequestedException(CANCEL_FORCEFUL)
                    // to CancellationException or CancellationByUserException
                    if (requestedTerminationMode == CANCEL_FORCEFUL) {
                        Throwable newFailure = userInitiatedTermination
                                ? new CancellationByUserException()
                                : new CancellationException();
                        newFailure.initCause(failure);
                        fail = newFailure;
                    }
                    jobCompletionFuture.completeExceptionally(fail);
                    jobEventService.publishEvent(jobId, RUNNING, FAILED, requestedTerminationMode != null
                                    ? requestedTerminationMode.actionAfterTerminate().description() : fail.toString(),
                            userInitiatedTermination);
                }
                jobEventService.removeAllEventListeners(jobId);
            } finally {
                lock.unlock();
            }
        });
    }

    public UUID addStatusListener(Registration registration) {
        lock.lock();
        try {
            if (jobCompletionFuture.isDone()) {
                throw cannotAddStatusListener(
                        jobCompletionFuture.isCompletedExceptionally() ? FAILED : COMPLETED);
            }
            return jobEventService.handleAllRegistrations(jobId, registration).getId();
        } finally {
            lock.unlock();
        }
    }

    private void invokeClose(@Nullable Throwable failure, ProcessorMetaSupplier metaSupplier) {
        try {
            ClassLoader classLoader = jobClassLoaderService.getClassLoader(jobId);
            doWithClassLoader(classLoader, () -> metaSupplier.close(failure));
        } catch (Throwable e) {
            logger.severe(jobIdString
                    + " encountered an exception in ProcessorMetaSupplier.complete(), ignoring it", e);
        }
    }

    private void cancelInvocations() {
        if (invocationsCancelled.compareAndSet(false, true)) {
            for (MemberInfo memberInfo : executionPlanMap.keySet()) {
                // Termination is fire and forget. If the termination isn't handled (e.g. due to a packet loss or
                // because the execution wasn't yet initialized at the target), it will be fixed by the
                // CheckLightJobsOperation.
                TerminateExecutionOperation op = new TerminateExecutionOperation(jobId, jobId, CANCEL_FORCEFUL);
                nodeEngine.getOperationService()
                        .createInvocationBuilder(JetServiceBackend.SERVICE_NAME, op, memberInfo.getAddress())
                        .invoke();
            }
        }
    }

    /**
     * @param completionCallback a consumer that will receive a list of
     *                           responses, one for each member, after all have
     *                           been received. The value will be either the
     *                           response (including a null response) or an
     *                           exception thrown from the operation; size will
     *                           be equal to participant count
     * @param errorCallback      A callback that will be called after each a
     *                           failure of each individual operation
     */
    private void invokeOnParticipants(
            Function<ExecutionPlan, Operation> operationCtor,
            @Nullable Consumer<Collection<Object>> completionCallback,
            @Nullable Consumer<Throwable> errorCallback
    ) {
        ConcurrentMap<Address, Object> responses = new ConcurrentHashMap<>();
        AtomicInteger remainingCount = new AtomicInteger(executionPlanMap.size());
        for (Entry<MemberInfo, ExecutionPlan> entry : executionPlanMap.entrySet()) {
            Address address = entry.getKey().getAddress();
            Operation op = operationCtor.apply(entry.getValue());
            invokeOnParticipant(address, op, completionCallback, errorCallback, responses, remainingCount);
        }
    }

    private void invokeOnParticipant(
            Address address,
            Operation op,
            @Nullable Consumer<Collection<Object>> completionCallback,
            @Nullable Consumer<Throwable> errorCallback,
            ConcurrentMap<Address, Object> collectedResponses,
            AtomicInteger remainingCount
    ) {
        InvocationFuture<Object> future = nodeEngine.getOperationService()
                .createInvocationBuilder(JetServiceBackend.SERVICE_NAME, op, address)
                .invoke();

        future.whenComplete((r, throwable) -> {
            Object response = r != null ? r : throwable != null ? peel(throwable) : NULL_OBJECT;
            if (throwable instanceof OperationTimeoutException) {
                logger.warning("Retrying " + op.getClass().getSimpleName() + " that failed with "
                        + OperationTimeoutException.class.getSimpleName() + " in " + jobIdString);
                invokeOnParticipant(address, op, completionCallback, errorCallback,
                        collectedResponses, remainingCount);
                return;
            }
            if (errorCallback != null && throwable != null) {
                errorCallback.accept(throwable);
            }
            Object oldResponse = collectedResponses.put(address, response);
            assert oldResponse == null :
                    "Duplicate response for " + address + ". Old=" + oldResponse + ", new=" + response;
            if (remainingCount.decrementAndGet() == 0 && completionCallback != null) {
                completionCallback.accept(collectedResponses.values().stream()
                        .map(o -> o == NULL_OBJECT ? null : o)
                        .collect(Collectors.toList()));
            }
        });
    }

    /**
     * Returns any error from a collection of responses. Ignores non-Throwable
     * responses. Exceptions other than {@link CancellationException} and
     * {@link JobTerminateRequestedException} take precedence.
     */
    private Throwable findError(Collection<Object> responses) {
        Throwable result = null;
        for (Object response : responses) {
            if (response instanceof Throwable
                    && (result == null
                    || result instanceof JobTerminateRequestedException
                    || result instanceof CancellationException)
            ) {
                result = (Throwable) response;
            }
        }
        if (result != null
                && !(result instanceof CancellationException)
                && !(result instanceof JobTerminateRequestedException)) {
            // We must wrap the exception. Otherwise the error will become the error of the SubmitJobOp. And
            // if the error is, for example, MemberLeftException, the job will be resubmitted even though it
            // was already running. Fixes https://github.com/hazelcast/hazelcast/issues/18844
            result = new JetException("Execution on a member failed: " + result, result);
        }
        return result;
    }

    public void requestTermination(boolean userInitiated) {
        this.userInitiatedTermination = userInitiated;
        cancelInvocations();
    }

    public boolean isCancelled() {
        return invocationsCancelled.get();
    }

    public long getStartTime() {
        return startTime;
    }

    public CompletableFuture<Void> getCompletionFuture() {
        return jobCompletionFuture;
    }

    public JobConfig getJobConfig() {
        return jobConfig;
    }
}
