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
import com.hazelcast.cluster.Member;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.internal.util.Preconditions;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.impl.operation.GetJobIdsOperation;
import com.hazelcast.jet.impl.operation.GetJobIdsOperation.GetJobIdsResult;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.impl.NodeEngineImpl;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.security.auth.Subject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static com.hazelcast.cluster.memberselector.MemberSelectors.DATA_MEMBER_SELECTOR;
import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.jet.impl.util.ExceptionUtil.isOrHasCause;
import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static java.util.Collections.singleton;

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
    public Map<Address, GetJobIdsResult> getJobsInt(String onlyName, Long onlyJobId) {
        Map<Address, CompletableFuture<GetJobIdsResult>> futures = new HashMap<>();
        // if onlyName != null, only send the operation to master. Light jobs cannot have a name
        Collection<Member> targetMembers = onlyName == null
                ? nodeEngine.getClusterService().getMembers(DATA_MEMBER_SELECTOR)
                : singleton(nodeEngine.getClusterService().getMembers().iterator().next());

        GetJobIdsOperation masterOperation = new GetJobIdsOperation(onlyName, onlyJobId);

        CompletableFuture<GetJobIdsResult> masterFuture = nodeEngine
                .getOperationService()
                .createMasterInvocationBuilder(JetServiceBackend.SERVICE_NAME, masterOperation)
                .invoke();

        for (Member member : targetMembers) {
            GetJobIdsOperation operation = new GetJobIdsOperation(onlyName, onlyJobId);
            futures.put(member.getAddress(), nodeEngine
                    .getOperationService()
                    .createInvocationBuilder(JetServiceBackend.SERVICE_NAME, operation, member.getAddress())
                    .invoke());
        }

        Map<Address, GetJobIdsResult> res = new HashMap<>(futures.size());
        for (Entry<Address, CompletableFuture<GetJobIdsResult>> en : futures.entrySet()) {
            GetJobIdsResult result;
            try {
                result = en.getValue().get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                result = GetJobIdsResult.EMPTY;
            } catch (ExecutionException e) {
                // If we don't get a response from a non-master member, it can contain only light jobs - we ignore that
                // member's failure, because these jobs are not as important. If we don't get response from the master,
                // we report it to the user.
                if (isOrHasCause(e, MemberLeftException.class) || isOrHasCause(e, TargetNotMemberException.class)) {
                    result = GetJobIdsResult.EMPTY;
                } else {
                    throw new RuntimeException("Error when getting job IDs: " + e, e);
                }
            }

            res.put(en.getKey(), result);
        }

        res.put(null, filterNonLightJobs(masterFuture));
        return res;
    }

    private GetJobIdsResult filterNonLightJobs(CompletableFuture<GetJobIdsResult> masterFuture) {
        GetJobIdsResult result;
        try {
            result = masterFuture.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return GetJobIdsResult.EMPTY;
        } catch (Exception e) {
            // We do not ignore any exception from master.
            throw rethrow(e);
        }
        List<Tuple2<Long, Boolean>> nonLightJobs = new ArrayList<>();
        for (int i = 0; i < result.getJobIds().length; i++) {
            long jobId = result.getJobIds()[i];
            if (result.getIsLightJobs()[i]) {
                continue;
            }
            nonLightJobs.add(tuple2(jobId, false));
        }
        return new GetJobIdsResult(nonLightJobs);
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
