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

package com.hazelcast.jet.impl;

import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.impl.operation.GetJobIdsByNameOperation;
import com.hazelcast.jet.impl.operation.GetJobIdsOperation;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.concurrent.Future;

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
    public List<Job> getJobs() {
        Address masterAddress = nodeEngine.getMasterAddress();
        Future<List<Long>> future = nodeEngine
                .getOperationService()
                .createInvocationBuilder(JetService.SERVICE_NAME, new GetJobIdsOperation(), masterAddress)
                .invoke();

        try {
            return future.get()
                         .stream()
                         .map(jobId -> new JobProxy((NodeEngineImpl) nodeEngine, jobId))
                         .collect(toList());
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    @Override
    public List<Long> getJobIdsByName(String name) {
        Address masterAddress = nodeEngine.getMasterAddress();
        Future<List<Long>> future = nodeEngine
                .getOperationService()
                .createInvocationBuilder(JetService.SERVICE_NAME, new GetJobIdsByNameOperation(name), masterAddress)
                .invoke();

        try {
            return future.get();
        } catch (Throwable t) {
            throw rethrow(t);
        }
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
        return Util.existsDistributedObject(nodeEngine, serviceName, objectName);
    }

    @Override
    public Job newJobProxy(long jobId) {
        return new JobProxy((NodeEngineImpl) nodeEngine, jobId);
    }

    @Override
    public Job newJobProxy(long jobId, DAG dag, JobConfig config) {
        return new JobProxy((NodeEngineImpl) nodeEngine, jobId, dag, config);
    }

    @Override
    public ILogger getLogger() {
        return nodeEngine.getLogger(getClass());
    }
}
