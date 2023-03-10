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

import com.hazelcast.instance.impl.executejar.ExecuteJobParameters;
import com.hazelcast.jet.JetService;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.JobStateSnapshot;
import com.hazelcast.jet.Observable;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.pipeline.Pipeline;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;

@SuppressWarnings({"checkstyle:methodcount"})
public class BootstrappedJetProxy implements JetService {
    private final JetService jetService;

    private final ThreadLocal<ExecuteJobParameters> executeJobParametersThreadLocal = new ThreadLocal<>();

    BootstrappedJetProxy(@Nonnull JetService jetService) {
        this.jetService = jetService;
    }

    @Nonnull
    @Override
    public JetConfig getConfig() {
        return jetService.getConfig();
    }

    @Nonnull
    @Override
    public Job newJob(@Nonnull Pipeline pipeline, @Nonnull JobConfig jobConfig) {
        updateJobConfig(jobConfig);
        Job job = jetService.newJob(pipeline, jobConfig);
        addToSubmittedJobs(job);
        return job;
    }

    @Nonnull
    @Override
    public Job newJob(@Nonnull DAG dag, @Nonnull JobConfig jobConfig) {
        updateJobConfig(jobConfig);
        Job job = jetService.newJob(dag, jobConfig);
        addToSubmittedJobs(job);
        return job;
    }

    @Nonnull
    @Override
    public Job newJobIfAbsent(@Nonnull Pipeline pipeline, @Nonnull JobConfig jobConfig) {
        updateJobConfig(jobConfig);
        Job job = jetService.newJobIfAbsent(pipeline, jobConfig);
        addToSubmittedJobs(job);
        return job;
    }

    @Nonnull
    @Override
    public Job newJobIfAbsent(@Nonnull DAG dag, @Nonnull JobConfig jobConfig) {
        updateJobConfig(jobConfig);
        Job job = jetService.newJobIfAbsent(dag, jobConfig);
        addToSubmittedJobs(job);
        return job;
    }

    @Override
    public Job newLightJob(@Nonnull Pipeline pipeline, @Nonnull JobConfig jobConfig) {
        updateJobConfig(jobConfig);
        Job job = jetService.newLightJob(pipeline, jobConfig);
        addToSubmittedJobs(job);
        return job;
    }

    @Override
    public Job newLightJob(@Nonnull DAG dag, @Nonnull JobConfig jobConfig) {
        updateJobConfig(jobConfig);
        Job job = jetService.newLightJob(dag, jobConfig);
        addToSubmittedJobs(job);
        return job;
    }

    @Nonnull
    @Override
    public List<Job> getJobs(@Nonnull String name) {
        return jetService.getJobs(name);
    }

    @Nonnull
    @Override
    public <T> Observable<T> getObservable(@Nonnull String name) {
        return jetService.getObservable(name);
    }

    @Nonnull
    @Override
    public List<Job> getJobs() {
        return jetService.getJobs();
    }

    @Nullable
    @Override
    public Job getJob(long jobId) {
        return jetService.getJob(jobId);
    }

    @Nullable
    @Override
    public JobStateSnapshot getJobStateSnapshot(@Nonnull String name) {
        return jetService.getJobStateSnapshot(name);
    }

    @Nonnull
    @Override
    public Collection<JobStateSnapshot> getJobStateSnapshots() {
        return jetService.getJobStateSnapshots();
    }

    @Nonnull
    @Override
    public Collection<Observable<?>> getObservables() {
        return jetService.getObservables();
    }

    private void addToSubmittedJobs(@Nonnull Job job) {
        getThreadLocalParameters().addSubmittedJob(job);
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

    private void updateJobConfig(@Nonnull JobConfig jobConfig) {
        ExecuteJobParameters jobParameters = getThreadLocalParameters();
        jobConfig.addJar(jobParameters.getJarPath());

        if (jobParameters.hasSnapshotName()) {
            jobConfig.setInitialSnapshotName(jobParameters.getSnapshotName());
        }
        if (jobParameters.hasJobName()) {
            jobConfig.setName(jobParameters.getJobName());
        }
    }
}
