/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.internal.util.Preconditions;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.impl.operation.GetJobIdsOperation;
import com.hazelcast.jet.impl.operation.GetJobIdsOperation.GetJobIdsResult;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.impl.Invocation;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.security.auth.Subject;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

import static com.hazelcast.internal.util.InvocationUtil.invokeAndReduceOnAllClusterMembers;
import static com.hazelcast.jet.impl.operation.GetJobIdsOperation.ALL_JOBS;
import static com.hazelcast.jet.impl.util.ExceptionUtil.isOrHasCause;
import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static java.util.Objects.requireNonNullElse;
import static java.util.stream.Collectors.toMap;

/**
 * Member-side {@code JetInstance} implementation
 */
public class JetInstanceImpl extends AbstractJetInstance<Address> {
    private final NodeEngineImpl nodeEngine;
    private final JetConfig config;

    JetInstanceImpl(HazelcastInstanceImpl hazelcastInstance, JetConfig config) {
        super(hazelcastInstance);
        this.nodeEngine = hazelcastInstance.node.getNodeEngine();
        this.config = config;
    }

    @Nonnull
    @Override
    public JetConfig getConfig() {
        return config;
    }


    @Override
    public Address getMasterId() {
        return Preconditions.checkNotNull(nodeEngine.getMasterAddress(), "Cluster has not elected a master");
    }

    @Override
    protected Map<Address, GetJobIdsResult> getAllJobs() {
        return getJobsById(null);
    }

    @Override
    protected GetJobIdsResult getJobByName(String name) {
        GetJobIdsOperation masterOperation = new GetJobIdsOperation(name, ALL_JOBS, false);
        CompletableFuture<GetJobIdsResult> masterFuture = nodeEngine
                .getOperationService()
                .createMasterInvocationBuilder(JetServiceBackend.SERVICE_NAME, masterOperation)
                .invoke();
        return getJobIdsResultSafe(masterFuture);
    }

    @Override
    protected Map<Address, GetJobIdsResult> getJobsById(@Nullable Long jobId) {
        long jobIdParameter = requireNonNullElse(jobId, ALL_JOBS);
        GetJobIdsOperation masterOperation = new GetJobIdsOperation(null, jobIdParameter, false);

        Invocation<GetJobIdsResult> masterInvocation = nodeEngine
                .getOperationService()
                .createMasterInvocationBuilder(JetServiceBackend.SERVICE_NAME, masterOperation)
                .build();

        CompletableFuture<GetJobIdsResult> masterFuture = masterInvocation.invoke();

        Supplier<Operation> operationSupplier = () -> new GetJobIdsOperation(
                null,
                jobIdParameter,
                true
        );

        CompletableFuture<Map<Address, GetJobIdsResult>> allMembersFuture = invokeAndReduceOnAllClusterMembers(
                nodeEngine,
                operationSupplier,
                (result, e) -> {
                    if (e == null) {
                        return result;
                    }
                    if (isOrHasCause(e, MemberLeftException.class)
                            || isOrHasCause(e, TargetNotMemberException.class)
                            || isOrHasCause(e, HazelcastInstanceNotActiveException.class)) {
                        return GetJobIdsResult.EMPTY;
                    }
                    if (isOrHasCause(e, InterruptedException.class)) {
                        return GetJobIdsResult.EMPTY;
                    }
                    throw new JetException("Error when getting job IDs: " + e, e);
                },
                memberMap -> memberMap.entrySet().stream()
                        .collect(toMap(
                                        en -> en.getKey().getAddress(),
                                        en -> (GetJobIdsResult) en.getValue()
                                )
                        )
        );

        var masterResult = getJobIdsResultSafe(masterFuture);
        try {
            Map<Address, GetJobIdsResult> allMemberResults = allMembersFuture.get();
            allMemberResults.put(masterInvocation.getTargetAddress(), masterResult);
            return allMemberResults;
        } catch (InterruptedException | ExecutionException e) {
            throw rethrow(e);
        }
    }

    private GetJobIdsResult getJobIdsResultSafe(CompletableFuture<GetJobIdsResult> future) {
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return GetJobIdsResult.EMPTY;
        } catch (ExecutionException e) {
            throw rethrow(e);
        }
    }

    @Override
    public void shutdown() {
        try {
            JetServiceBackend jetServiceBackend = nodeEngine.getService(JetServiceBackend.SERVICE_NAME);
            jetServiceBackend.shutDownJobs();
            super.shutdown();
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    /**
     * Tells whether this member knows of the given object name.
     * <p>
     * Notes:
     * <ul><li>
     *     this member might not know it exists if the proxy creation operation went wrong
     * </li><li>
     *     this member might not know it was destroyed if the destroy operation went wrong
     * </li><li>
     *     it might be racy with respect to other create/destroy operations
     * </li></ul>
     *
     * @param serviceName for example, {@link MapService#SERVICE_NAME}
     * @param objectName  object name
     * @return true, if this member knows of the object
     */
    @Override
    public boolean existsDistributedObject(@Nonnull String serviceName, @Nonnull String objectName) {
        return nodeEngine.getProxyService().existsDistributedObject(serviceName, objectName);
    }

    @Override
    public Job newJobProxy(long jobId, Address lightJobCoordinator) {
        return new JobProxy(nodeEngine, jobId, lightJobCoordinator);
    }

    @Override
    public Job newJobProxy(long jobId,
                           boolean isLightJob,
                           @Nonnull Object jobDefinition,
                           @Nonnull JobConfig config,
                           @Nullable Subject subject) {
        return new JobProxy(nodeEngine, jobId, isLightJob, jobDefinition, config, subject);
    }

    @Override
    public ILogger getLogger() {
        return nodeEngine.getLogger(getClass());
    }

}
