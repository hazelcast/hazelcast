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

package com.hazelcast.instance.impl.executejar;

import com.hazelcast.instance.impl.BootstrappedJetProxy;
import com.hazelcast.instance.impl.executejar.jetservicedecorator.memberside.BootstrapJobDecorator;
import com.hazelcast.jet.JetService;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.pipeline.Pipeline;

import javax.annotation.Nonnull;

/**
 * This class' state holds {@link ExecuteJobParameters parameters} used by Jet jobs.
 * State is thread-local,so this proxy can be used by multiple threads.
 */
public class MemberJetProxy<M> extends BootstrappedJetProxy<M> {
    private final ThreadLocal<ExecuteJobParameters> executeJobParametersThreadLocal =
            ThreadLocal.withInitial(ExecuteJobParameters::new);

    public MemberJetProxy(@Nonnull JetService jetService) {
        super(jetService);
    }

    public boolean hasExecuteJobParameters() {
        return getExecuteJobParameters() != null;
    }

    @Override
    public ExecuteJobParameters getExecuteJobParameters() {
        return executeJobParametersThreadLocal.get();
    }

    @Override
    public void setExecuteJobParameters(ExecuteJobParameters executeJobParameters) {
        executeJobParametersThreadLocal.set(executeJobParameters);
    }

    @Override
    public void removeExecuteJobParameters() {
        executeJobParametersThreadLocal.remove();
    }

    @Nonnull
    @Override
    public Job newJob(@Nonnull Pipeline pipeline, @Nonnull JobConfig config) {
        Job job = super.newJob(pipeline, config);
        return new BootstrapJobDecorator(job);
    }

    @Nonnull
    @Override
    public Job newJob(@Nonnull DAG dag, @Nonnull JobConfig config) {
        Job job = super.newJob(dag, config);
        return new BootstrapJobDecorator(job);
    }

    @Nonnull
    @Override
    public Job newJobIfAbsent(@Nonnull Pipeline pipeline, @Nonnull JobConfig config) {
        Job job = super.newJobIfAbsent(pipeline, config);
        return new BootstrapJobDecorator(job);
    }

    @Nonnull
    @Override
    public Job newJobIfAbsent(@Nonnull DAG dag, @Nonnull JobConfig config) {
        Job job = super.newJobIfAbsent(dag, config);
        return new BootstrapJobDecorator(job);
    }
}
