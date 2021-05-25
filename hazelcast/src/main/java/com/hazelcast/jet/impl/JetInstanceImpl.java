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
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.internal.util.Preconditions;
import com.hazelcast.jet.BasicJob;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.impl.operation.GetJobIdsOperation;
import com.hazelcast.jet.impl.operation.GetJobIdsOperation.GetJobIdsResult;
import com.hazelcast.jet.impl.util.ImdgUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.impl.InvocationFuture;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;

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

    @Nonnull @Override
    public JetConfig getConfig() {
        return config;
    }

    @Override
    public Address getMasterId() {
        return Preconditions.checkNotNull(nodeEngine.getMasterAddress(), "Cluster has not elected a master");
    }

    @Override
    public Map<Address, CompletableFuture<GetJobIdsResult>> getJobsInt(Long onlyJobId) {
        Map<Address, CompletableFuture<GetJobIdsResult>> futures = new HashMap<>();
        for (Member member : getCluster().getMembers()) {
            GetJobIdsOperation operation = new GetJobIdsOperation(onlyJobId);
            InvocationFuture<GetJobIdsResult> future = nodeEngine
                    .getOperationService()
                    .createInvocationBuilder(JetService.SERVICE_NAME, operation, member.getAddress())
                    .invoke();
            futures.put(member.getAddress(), future);
        }
        return futures;
    }

    @Nonnull @Override
    public List<Job> getJobs(@NotNull String name) {
        GetJobIdsOperation operation = new GetJobIdsOperation(name);
        Address masterAddress = getMasterId();
        InvocationFuture<GetJobIdsResult> future = nodeEngine
                .getOperationService()
                .createInvocationBuilder(JetService.SERVICE_NAME, operation, masterAddress)
                .invoke();

        GetJobIdsResult result;
        try {
            result = future.get();
        } catch (Throwable e) {
            throw rethrow(e);
        }
        List<Job> resultList = new ArrayList<>();
        for (int i = 0; i < result.getJobIds().length; i++) {
            resultList.add(new JobProxy(nodeEngine, result.getJobIds()[i], masterAddress));
        }
        return resultList;
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
    public BasicJob newJobProxy(long jobId, Address coordinator) {
        return new JobProxy(nodeEngine, jobId, coordinator);
    }

    @Override
    public BasicJob newJobProxy(long jobId, boolean isLightJob, Object jobDefinition, JobConfig config) {
        return new JobProxy(nodeEngine, jobId, isLightJob, jobDefinition, config);
    }

    @Override
    public ILogger getLogger() {
        return nodeEngine.getLogger(getClass());
    }

}
