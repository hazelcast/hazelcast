/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.operation;

import com.hazelcast.cluster.Address;
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.internal.cluster.MemberInfo;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.cluster.impl.MembersView;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.impl.JetService;
import com.hazelcast.jet.impl.Timers;
import com.hazelcast.jet.impl.execution.init.ExecutionPlan;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.impl.InvocationFuture;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.hazelcast.jet.Util.idToString;
import static com.hazelcast.jet.impl.TerminationMode.CANCEL_FORCEFUL;
import static com.hazelcast.jet.impl.execution.init.ExecutionPlanBuilder.createExecutionPlans;
import static com.hazelcast.jet.impl.util.ExceptionUtil.peel;
import static com.hazelcast.jet.impl.util.LoggingUtil.logFine;

public class LightMasterContext {

    private static final Object NULL_OBJECT = new Object() {
        @Override
        public String toString() {
            return "NULL_OBJECT";
        }
    };

    private static final JobConfig LIGHT_JOB_CONFIG = new JobConfig()
            .setMetricsEnabled(false)
            .setAutoScaling(false);

    private final NodeEngine nodeEngine;
    private final DAG dag;
    private final long jobId;

    private final ILogger logger;
    private final String jobIdString;

    private Map<MemberInfo, ExecutionPlan> executionPlanMap;
    private final AtomicBoolean invocationsCancelled = new AtomicBoolean();
    private final CompletableFuture<Void> jobCompletionFuture = new CompletableFuture<>();
    private Set<Vertex> vertices;

    LightMasterContext(NodeEngine nodeEngine, DAG dag, long jobId) {
        this.nodeEngine = nodeEngine;
        this.dag = dag;
        this.jobId = jobId;

        logger = nodeEngine.getLogger(LightMasterContext.class);
        jobIdString = idToString(jobId);
    }

    public CompletableFuture<Void> start() {
        Timers.i().lightMasterContext_start.start();
        MembersView membersView = getMembersView();
        if (logger.isFineEnabled()) {
            String dotRepresentation = dag.toDotString();
            logFine(logger, "Start executing light %s, execution graph in DOT format:\n%s"
                            + "\nHINT: You can use graphviz or http://viz-js.com to visualize the printed graph.",
                    jobIdString, dotRepresentation);
            logFine(logger, "Building execution plan for %s", jobIdString);
        }

        executionPlanMap = createExecutionPlans(nodeEngine, membersView, dag, jobId, jobId, LIGHT_JOB_CONFIG, 0, true);
        logFine(logger, "Built execution plans for %s", jobIdString);
        Set<MemberInfo> participants = executionPlanMap.keySet();
        Function<ExecutionPlan, Operation> operationCtor = plan -> {
            Timers.i().lightMasterContext_serializeOnePlan.start();
            Data serializedPlan = nodeEngine.getSerializationService().toData(plan);
            Timers.i().lightMasterContext_serializeOnePlan.stop();

            return new InitExecutionOperation(jobId, jobId, membersView.getVersion(), participants,
                    serializedPlan, true);
        };
        vertices = new HashSet<>();
        dag.iterator().forEachRemaining(vertices::add);
        invokeOnParticipants(operationCtor,
                responses -> finalizeJob(firstError(responses)),
                error -> cancelInvocations(),
                false);
        Timers.i().lightMasterContext_start.stop();
        return jobCompletionFuture;
    }

    private void finalizeJob(@Nullable Throwable failure) {
        Timers.i().initResponseTime.stop();
        // close ProcessorMetaSuppliers
        if (vertices != null) {
            for (Vertex vertex : vertices) {
                try {
                    vertex.getMetaSupplier().close(failure);
                } catch (Exception e) {
                    logger.severe(jobIdString
                            + " encountered an exception in ProcessorMetaSupplier.complete(), ignoring it", e);
                }
            }
        }

        if (failure == null) {
            jobCompletionFuture.complete(null);
        } else {
            jobCompletionFuture.completeExceptionally(failure);
        }
    }

    private void cancelInvocations() {
        if (invocationsCancelled.compareAndSet(false, true)) {
            nodeEngine.getExecutionService().execute(ExecutionService.ASYNC_EXECUTOR, () ->
                    invokeOnParticipants(plan -> new TerminateExecutionOperation(jobId, jobId, CANCEL_FORCEFUL),
                            responses -> {
                                if (responses.stream().anyMatch(Objects::nonNull)) {
                                    // log errors
                                    logger.severe(jobIdString + ": some TerminateExecutionOperation invocations " +
                                            "failed, execution might remain stuck: " + responses);
                                }
                            }, null, true));
        }
    }

    /**
     * @param completionCallback a consumer that will receive a list of
     *                           responses, one for each member, after all have
     *                           been received. The value will be either the
     *                           response (including a null response) or an
     *                           exception thrown from the operation; size will
     *                           be equal to participant count
     * @param errorCallback A callback that will be called after each a
     *                     failure of each individual operation
     * @param retryOnTimeoutException if true, operations that threw {@link
     *      OperationTimeoutException} will be retried
     */
    private void invokeOnParticipants(
            Function<ExecutionPlan, Operation> operationCtor,
            @Nullable Consumer<Collection<Object>> completionCallback,
            @Nullable Consumer<Throwable> errorCallback,
            boolean retryOnTimeoutException
    ) {
        ConcurrentMap<Address, Object> responses = new ConcurrentHashMap<>();
        AtomicInteger remainingCount = new AtomicInteger(executionPlanMap.size());
        for (Entry<MemberInfo, ExecutionPlan> entry : executionPlanMap.entrySet()) {
            Address address = entry.getKey().getAddress();
            Operation op = operationCtor.apply(entry.getValue());
            invokeOnParticipant(address, op, completionCallback, errorCallback, retryOnTimeoutException, responses,
                    remainingCount);
        }
    }

    private void invokeOnParticipant(
            Address address,
            Operation op,
            @Nullable Consumer<Collection<Object>> completionCallback,
            @Nullable Consumer<Throwable> errorCallback,
            boolean retryOnTimeoutException,
            ConcurrentMap<Address, Object> collectedResponses,
            AtomicInteger remainingCount
    ) {
        InvocationFuture<Object> future = nodeEngine.getOperationService()
                                                             .createInvocationBuilder(JetService.SERVICE_NAME, op, address)
                                                             .invoke();

        future.whenComplete((r, throwable) -> {
            Object response = r != null ? r : throwable != null ? peel(throwable) : NULL_OBJECT;
            if (retryOnTimeoutException && throwable instanceof OperationTimeoutException) {
                logger.warning("Retrying " + op.getClass().getSimpleName() + " that failed with "
                        + OperationTimeoutException.class.getSimpleName() + " in " + jobIdString);
                invokeOnParticipant(address, op, completionCallback, errorCallback, retryOnTimeoutException,
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

    private MembersView getMembersView() {
        ClusterServiceImpl clusterService = (ClusterServiceImpl) nodeEngine.getClusterService();
        return clusterService.getMembershipManager().getMembersView();
    }

    private Throwable firstError(Collection<Object> responses) {
        return responses.stream().filter(Throwable.class::isInstance).map(Throwable.class::cast)
                        .findFirst().orElse(null);
    }
}
