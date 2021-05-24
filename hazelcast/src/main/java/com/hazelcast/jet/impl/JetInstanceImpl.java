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

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.util.Preconditions;
import com.hazelcast.jet.BasicJob;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.impl.operation.GetJobIdsOperation;
import com.hazelcast.jet.impl.operation.SubmitJobOperation;
import com.hazelcast.jet.impl.util.ImdgUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.impl.InvocationFuture;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.LongStream;

import static com.hazelcast.cluster.memberselector.MemberSelectors.DATA_MEMBER_SELECTOR;
import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static java.util.stream.Collectors.toList;

/**
 * Member-side {@code JetInstance} implementation
 */
public class JetInstanceImpl extends AbstractJetInstance {
    private final NodeEngine nodeEngine;
    private final JetConfig config;

    JetInstanceImpl(HazelcastInstanceImpl hazelcastInstance, JetConfig config) {
        super(hazelcastInstance);
        this.nodeEngine = hazelcastInstance.node.getNodeEngine();
        this.config = config;
    }

    @Nonnull @Override
    public JetConfig getConfig() {
        return config;
    }

    @Nonnull @Override
    public BasicJob newLightJobInt(Object jobDefinition) {
        Address coordinatorAddress;
        if (nodeEngine.getLocalMember().isLiteMember()) {
            // on lite member forward the request to a random member
            Member[] members = nodeEngine.getClusterService().getMembers(DATA_MEMBER_SELECTOR).toArray(new Member[0]);
            coordinatorAddress = members[ThreadLocalRandom.current().nextInt(members.length)].getAddress();
        } else {
            coordinatorAddress = nodeEngine.getThisAddress();
        }

        long jobId = newJobId();
        Data serializedJobDefinition = nodeEngine.getSerializationService().toData(jobDefinition);
        SubmitJobOperation operation = new SubmitJobOperation(jobId, serializedJobDefinition, null, true);
        CompletableFuture<Void> future = nodeEngine
                .getOperationService()
                .createInvocationBuilder(JetService.SERVICE_NAME, operation, coordinatorAddress)
                .invoke();

        return new LightJobProxy(nodeEngine, jobId, coordinatorAddress, future);
    }

    @Nonnull @Override
    public List<BasicJob> getAllJobs() {
        List<CompletableFuture<long[]>> futures = new ArrayList<>();
        Address masterAddress = getMasterAddress();
        CompletableFuture<long[]> masterFuture = null;
        for (Member member : getCluster().getMembers()) {
            InvocationFuture<long[]> future = nodeEngine
                    .getOperationService()
                    .createInvocationBuilder(JetService.SERVICE_NAME, new GetJobIdsOperation(), member.getAddress())
                    .invoke();
            if (member.getAddress().equals(masterAddress)) {
                masterFuture = future;
            }
            futures.add(future);
        }

        try {
            CompletableFuture<long[]> finalMasterFuture = masterFuture;
            return futures
                    .stream().flatMapToLong(future -> {
                        try {
                            return Arrays.stream(future.get());
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return LongStream.of();
                        } catch (ExecutionException e) {
                            // Don't ignore exceptions from master. If we don't get a response from a non-master member, it
                            // can contain only light jobs - we ignore that member's failure, because these jobs are not as
                            // important. If we don't get response from the master, we report it to the user.
                            if (future != finalMasterFuture
                                    && (e.getCause() instanceof TargetNotMemberException || e.getCause() instanceof MemberLeftException)) {
                                return LongStream.of();
                            }
                            throw new RuntimeException("Error when getting job IDs: " + e, e);
                        }
                    })
                    .mapToObj(jobId -> new JobProxy((NodeEngineImpl) nodeEngine, jobId))
                    .collect(toList());
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    @Override
    public List<Long> getJobIdsByName(String name) {
        Address masterAddress = getMasterAddress();
        Future<List<Long>> future = nodeEngine
                .getOperationService()
                .createInvocationBuilder(JetService.SERVICE_NAME, new GetJobIdsOperation(name), masterAddress)
                .invoke();

        try {
            return future.get();
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    @Nonnull
    private Address getMasterAddress() {
        return Preconditions.checkNotNull(nodeEngine.getMasterAddress(), "Cluster has not elected a master");
    }

    @Override
    public void shutdown() {
        try {
            JetService jetService = nodeEngine.getService(JetService.SERVICE_NAME);
            jetService.shutDownJobs();
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
        return ImdgUtil.existsDistributedObject(nodeEngine, serviceName, objectName);
    }

    @Override
    public Job newJobProxy(long jobId) {
        return new JobProxy((NodeEngineImpl) nodeEngine, jobId);
    }

    @Override
    public Job newJobProxy(long jobId, Object jobDefinition, JobConfig config) {
        return new JobProxy((NodeEngineImpl) nodeEngine, jobId, jobDefinition, config);
    }

    @Override
    public ILogger getLogger() {
        return nodeEngine.getLogger(getClass());
    }

}
