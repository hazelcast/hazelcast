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
import java.util.concurrent.CopyOnWriteArrayList;

@SuppressWarnings({"checkstyle:methodcount"})
public class BootstrappedJetProxy implements JetService {
    private final JetService jet;
    private final CopyOnWriteArrayList<Job> submittedJobs = new CopyOnWriteArrayList<>();

    private final ThreadLocal<ExecuteJobParameters> executeJobParametersThreadLocal = new ThreadLocal<>();

    BootstrappedJetProxy(@Nonnull JetService jet) {
        this.jet = jet;
    }


    @Nonnull
    @Override
    public JetConfig getConfig() {
        return jet.getConfig();
    }

    @Nonnull
    @Override
    public Job newJob(@Nonnull Pipeline pipeline, @Nonnull JobConfig config) {
        return addToSubmittedJobs(jet.newJob(pipeline, updateJobConfig(config)));
    }

    @Nonnull
    @Override
    public Job newJob(@Nonnull DAG dag, @Nonnull JobConfig config) {
        return addToSubmittedJobs(jet.newJob(dag, updateJobConfig(config)));
    }

    @Nonnull
    @Override
    public Job newJobIfAbsent(@Nonnull Pipeline pipeline, @Nonnull JobConfig config) {
        return addToSubmittedJobs(jet.newJobIfAbsent(pipeline, updateJobConfig(config)));
    }

    @Nonnull
    @Override
    public Job newJobIfAbsent(@Nonnull DAG dag, @Nonnull JobConfig config) {
        return addToSubmittedJobs(jet.newJobIfAbsent(dag, updateJobConfig(config)));
    }

    @Override
    public Job newLightJob(@Nonnull Pipeline pipeline, @Nonnull JobConfig config) {
        return addToSubmittedJobs(jet.newLightJob(pipeline, updateJobConfig(config)));
    }

    @Override
    public Job newLightJob(@Nonnull DAG dag, @Nonnull JobConfig config) {
        return addToSubmittedJobs(jet.newLightJob(dag, updateJobConfig(config)));
    }

    @Nonnull
    @Override
    public List<Job> getJobs(@Nonnull String name) {
        return jet.getJobs(name);
    }

    @Nonnull
    @Override
    public <T> Observable<T> getObservable(@Nonnull String name) {
        return jet.getObservable(name);
    }

    @Nonnull
    @Override
    public List<Job> getJobs() {
        return jet.getJobs();
    }

    @Nullable
    @Override
    public Job getJob(long jobId) {
        return jet.getJob(jobId);
    }

    @Nullable
    @Override
    public JobStateSnapshot getJobStateSnapshot(@Nonnull String name) {
        return jet.getJobStateSnapshot(name);
    }

    @Nonnull
    @Override
    public Collection<JobStateSnapshot> getJobStateSnapshots() {
        return jet.getJobStateSnapshots();
    }

    @Nonnull
    @Override
    public Collection<Observable<?>> getObservables() {
        return jet.getObservables();
    }

    public void clearSubmittedJobs() {
        submittedJobs.clear();
    }

    @Nonnull
    public List<Job> submittedJobs() {
        return submittedJobs;
    }

    private Job addToSubmittedJobs(@Nonnull Job job) {
        submittedJobs.add(job);
        return job;
    }

    public ExecuteJobParameters getExecuteJobParameters() {
        return executeJobParametersThreadLocal.get();
    }

    public void setExecuteJobParameters(ExecuteJobParameters parameters) {
        executeJobParametersThreadLocal.set(parameters);
    }

    private JobConfig updateJobConfig(@Nonnull JobConfig config) {
        ExecuteJobParameters jobParameters = getExecuteJobParameters();
        config.addJar(jobParameters.getJarPath());

        if (jobParameters.hasSnapshotName()) {
            config.setInitialSnapshotName(jobParameters.getSnapshotName());
        }
        if (jobParameters.hasJobName()) {
            config.setName(jobParameters.getJobName());
        }
        // Remove the parameters when no longer used
        executeJobParametersThreadLocal.remove();
        return config;
    }
}
