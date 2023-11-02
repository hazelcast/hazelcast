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

package com.hazelcast.instance.impl;

import com.hazelcast.cluster.Cluster;
import com.hazelcast.collection.IList;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.executejar.ExecuteJobParameters;
import com.hazelcast.jet.JetCacheManager;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.JetService;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.Observable;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.impl.AbstractJetInstance;
import com.hazelcast.jet.impl.operation.GetJobIdsOperation;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.IMap;
import com.hazelcast.replicatedmap.ReplicatedMap;
import com.hazelcast.topic.ITopic;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.security.auth.Subject;
import java.util.List;
import java.util.Map;

/**
 * This class is a decorator that delegates most of the calls to given JetService.
 * Implementors of this class provides a strategy pattern to access ExecuteJobParameters to launch a new jet job
 */
@SuppressWarnings({"checkstyle:methodcount"})
public abstract class BootstrappedJetProxy<M> extends AbstractJetInstance<M> {

    private static final ILogger LOGGER = Logger.getLogger(BootstrappedJetProxy.class);

    private final AbstractJetInstance<M> jetInstance;

    protected BootstrappedJetProxy(@Nonnull JetService jetService) {
        super(((AbstractJetInstance) jetService).getHazelcastInstance());
        this.jetInstance = (AbstractJetInstance<M>) jetService;
    }

    public abstract boolean hasExecuteJobParameters();

    /**
     * The strategy to get ExecuteJobParameters on client and member side
     */
    public abstract ExecuteJobParameters getExecuteJobParameters();

    /**
     * The strategy to set ExecuteJobParameters on client and member side
     */
    public abstract void setExecuteJobParameters(ExecuteJobParameters executeJobParameters);

    public void removeExecuteJobParameters() {
        // empty
    }

    @Nonnull
    @Override
    public String getName() {
        return jetInstance.getName();
    }

    @Nonnull
    @Override
    public HazelcastInstance getHazelcastInstance() {
        return jetInstance.getHazelcastInstance();
    }

    @Nonnull
    @Override
    public Cluster getCluster() {
        return jetInstance.getCluster();
    }

    @Nonnull
    @Override
    public JetConfig getConfig() {
        return jetInstance.getConfig();
    }

    @Nonnull
    @Override
    public Job newJob(@Nonnull Pipeline pipeline, @Nonnull JobConfig config) {
        updateJobConfig(config);
        Job job = jetInstance.newJob(pipeline, config);
        addToSubmittedJobs(job);
        return job;
    }

    @Nonnull
    @Override
    public Job newJob(@Nonnull DAG dag, @Nonnull JobConfig config) {
        updateJobConfig(config);
        Job job = jetInstance.newJob(dag, config);
        addToSubmittedJobs(job);
        return job;
    }

    @Nonnull
    @Override
    public Job newJobIfAbsent(@Nonnull Pipeline pipeline, @Nonnull JobConfig config) {
        updateJobConfig(config);
        Job job = jetInstance.newJobIfAbsent(pipeline, config);
        addToSubmittedJobs(job);
        return job;
    }

    @Nonnull
    @Override
    public Job newJobIfAbsent(@Nonnull DAG dag, @Nonnull JobConfig config) {
        updateJobConfig(config);
        Job job = jetInstance.newJobIfAbsent(dag, config);
        addToSubmittedJobs(job);
        return job;
    }

    @Nonnull
    @Override
    public List<Job> getJobs(@Nonnull String name) {
        return jetInstance.getJobs(name);
    }

    @Nonnull
    @Override
    public <K, V> IMap<K, V> getMap(@Nonnull String name) {
        return jetInstance.getMap(name);
    }

    @Nonnull
    @Override
    public <K, V> ReplicatedMap<K, V> getReplicatedMap(@Nonnull String name) {
        return jetInstance.getReplicatedMap(name);
    }


    // supress "@Deprecated" code should not be used
    @SuppressWarnings("java:S1874")
    @Nonnull
    @Override
    public JetCacheManager getCacheManager() {
        return jetInstance.getCacheManager();
    }

    @Nonnull
    @Override
    public <E> IList<E> getList(@Nonnull String name) {
        return jetInstance.getList(name);
    }

    @Nonnull
    @Override
    public <T> ITopic<T> getReliableTopic(@Nonnull String name) {
        return jetInstance.getReliableTopic(name);
    }

    @Nonnull
    @Override
    public <T> Observable<T> getObservable(@Nonnull String name) {
        return jetInstance.getObservable(name);
    }

    @Override
    public void shutdown() {
        jetInstance.shutdown();
    }

    @Override
    public boolean existsDistributedObject(@Nonnull String serviceName, @Nonnull String objectName) {
        return jetInstance.existsDistributedObject(serviceName, objectName);
    }

    @Override
    public ILogger getLogger() {
        return jetInstance.getLogger();
    }

    @Override
    public Job newJobProxy(long jobId, M lightJobCoordinator) {
        return jetInstance.newJobProxy(jobId, lightJobCoordinator);
    }

    @Override
    public Job newJobProxy(long jobId,
                           boolean isLightJob,
                           @Nonnull Object jobDefinition,
                           @Nonnull JobConfig config,
                           @Nullable Subject subject) {
        return jetInstance.newJobProxy(jobId, isLightJob, jobDefinition, config, subject);
    }

    @Override
    public M getMasterId() {
        return jetInstance.getMasterId();
    }

    @Override
    public Map<M, GetJobIdsOperation.GetJobIdsResult> getJobsInt(String onlyName, Long onlyJobId) {
        return jetInstance.getJobsInt(onlyName, onlyJobId);
    }

    private void addToSubmittedJobs(@Nonnull Job job) {
        if (hasExecuteJobParameters()) {
            ExecuteJobParameters executeJobParameters = getExecuteJobParameters();
            executeJobParameters.addSubmittedJob(job);
        }
    }

    private void updateJobConfig(JobConfig jobConfig) {
        if (hasExecuteJobParameters()) {
            ExecuteJobParameters jobParameters = getExecuteJobParameters();

            if (jobParameters.hasJarPath()) {
                jobConfig.addJar(jobParameters.getJarPath());

                if (jobParameters.hasSnapshotName()) {
                    jobConfig.setInitialSnapshotName(jobParameters.getSnapshotName());
                }
                if (jobParameters.hasJobName()) {
                    jobConfig.setName(jobParameters.getJobName());
                }
            } else {
                String message = "The jet job has been started from a thread that is different from the one that called "
                                 + "the main method. \n"
                                 + "The job could not be found in the ThreadLocal and the job will not start.\n"
                                 + "If you still want to start job in a different thread, then you need to set the parameters "
                                 + "of the JobConfig in that thread\n"
                                 + "JobConfig\n  .addJar(...)\n  .setInitialSnapshotName(...)\n  .setName(...); ";
                LOGGER.severe(message);
                throw new JetException(message);
            }
        }
    }
}
