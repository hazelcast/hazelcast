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
import com.hazelcast.map.IMap;
import com.hazelcast.replicatedmap.ReplicatedMap;
import com.hazelcast.topic.ITopic;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;

/**
 * This class is a stateful proxy that delegates calls to given JetService.
 * The state is about running a jet job, and it stored in a ThreadLocal object
 */
@SuppressWarnings({"checkstyle:methodcount"})
public class BootstrappedJetProxy<M> extends AbstractJetInstance<M> {
    private final AbstractJetInstance<M> jet;
    private final ThreadLocal<ExecuteJobParameters> executeJobParametersThreadLocal =
            ThreadLocal.withInitial(ExecuteJobParameters::new);

    BootstrappedJetProxy(@Nonnull JetService jet) {
        super(((AbstractJetInstance) jet).getHazelcastInstance());
        this.jet = (AbstractJetInstance<M>) jet;
    }

    public ExecuteJobParameters getThreadLocalParameters() {
        return executeJobParametersThreadLocal.get();
    }

    public void setThreadLocalParameters(ExecuteJobParameters parameters) {
        executeJobParametersThreadLocal.set(parameters);
    }

    public void removeThreadLocalParameters() {
        executeJobParametersThreadLocal.remove();
    }

    @Nonnull
    @Override
    public String getName() {
        return jet.getName();
    }

    @Nonnull
    @Override
    public HazelcastInstance getHazelcastInstance() {
        return jet.getHazelcastInstance();
    }

    @Nonnull
    @Override
    public Cluster getCluster() {
        return jet.getCluster();
    }

    @Nonnull
    @Override
    public JetConfig getConfig() {
        return jet.getConfig();
    }

    @Nonnull
    @Override
    public Job newJob(@Nonnull Pipeline pipeline, @Nonnull JobConfig config) {
        updateJobConfig(config);
        Job job = jet.newJob(pipeline, config);
        addToSubmittedJobs(job);
        return job;
    }

    @Nonnull
    @Override
    public Job newJob(@Nonnull DAG dag, @Nonnull JobConfig config) {
        updateJobConfig(config);
        Job job = jet.newJob(dag, config);
        addToSubmittedJobs(job);
        return job;
    }

    @Nonnull
    @Override
    public Job newJobIfAbsent(@Nonnull Pipeline pipeline, @Nonnull JobConfig config) {
        updateJobConfig(config);
        Job job = jet.newJobIfAbsent(pipeline, config);
        addToSubmittedJobs(job);
        return job;
    }

    @Nonnull
    @Override
    public Job newJobIfAbsent(@Nonnull DAG dag, @Nonnull JobConfig config) {
        updateJobConfig(config);
        Job job = jet.newJobIfAbsent(dag, config);
        addToSubmittedJobs(job);
        return job;
    }

    @Nonnull
    @Override
    public List<Job> getJobs(@Nonnull String name) {
        return jet.getJobs(name);
    }

    @Nonnull
    @Override
    public <K, V> IMap<K, V> getMap(@Nonnull String name) {
        return jet.getMap(name);
    }

    @Nonnull
    @Override
    public <K, V> ReplicatedMap<K, V> getReplicatedMap(@Nonnull String name) {
        return jet.getReplicatedMap(name);
    }


    // supress "@Deprecated" code should not be used
    @SuppressWarnings("java:S1874")
    @Nonnull
    @Override
    public JetCacheManager getCacheManager() {
        return jet.getCacheManager();
    }

    @Nonnull
    @Override
    public <E> IList<E> getList(@Nonnull String name) {
        return jet.getList(name);
    }

    @Nonnull
    @Override
    public <T> ITopic<T> getReliableTopic(@Nonnull String name) {
        return jet.getReliableTopic(name);
    }

    @Nonnull
    @Override
    public <T> Observable<T> getObservable(@Nonnull String name) {
        return jet.getObservable(name);
    }

    @Override
    public void shutdown() {
        jet.shutdown();
    }

    @Override
    public boolean existsDistributedObject(@Nonnull String serviceName, @Nonnull String objectName) {
        return jet.existsDistributedObject(serviceName, objectName);
    }

    @Override
    public ILogger getLogger() {
        return jet.getLogger();
    }

    @Override
    public Job newJobProxy(long jobId, M lightJobCoordinator) {
        return jet.newJobProxy(jobId, lightJobCoordinator);
    }

    @Override
    public Job newJobProxy(long jobId, boolean isLightJob, @Nonnull Object jobDefinition, @Nonnull JobConfig config) {
        return jet.newJobProxy(jobId, isLightJob, jobDefinition, config);
    }

    @Override
    public M getMasterId() {
        return jet.getMasterId();
    }

    @Override
    public Map<M, GetJobIdsOperation.GetJobIdsResult> getJobsInt(String onlyName, Long onlyJobId) {
        return jet.getJobsInt(onlyName, onlyJobId);
    }

    private void addToSubmittedJobs(@Nonnull Job job) {
        getThreadLocalParameters().addSubmittedJob(job);
    }

    private void updateJobConfig(JobConfig jobConfig) {
        ExecuteJobParameters jobParameters = getThreadLocalParameters();

        if (jobParameters.hasJarPath()) {
            jobConfig.addJar(jobParameters.getJarPath());

            if (jobParameters.hasSnapshotName()) {
                jobConfig.setInitialSnapshotName(jobParameters.getSnapshotName());
            }
            if (jobParameters.hasJobName()) {
                jobConfig.setName(jobParameters.getJobName());
            }
        }
    }
}
