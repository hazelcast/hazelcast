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

package com.hazelcast.instance.impl.executejar.jetservicedecorator.memberside;

import com.hazelcast.jet.JetException;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.JobStateSnapshot;
import com.hazelcast.jet.JobStatusListener;
import com.hazelcast.jet.config.DeltaJobConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.core.JobSuspensionCause;
import com.hazelcast.jet.core.metrics.JobMetrics;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * This class decorates a Job and changes behavior.
 * For example, it does not allow a blocking join() method
 */
public class BootstrapJobDecorator implements Job {

    private final Job job;

    public BootstrapJobDecorator(Job job) {
        this.job = job;
    }

    @Override
    public boolean isLightJob() {
        return job.isLightJob();
    }

    @Override
    public long getId() {
        return job.getId();
    }

    @Nonnull
    @Override
    public CompletableFuture<Void> getFuture() {
        return job.getFuture();
    }

    @Override
    public void cancel() {
        job.cancel();
    }

    @Override
    public long getSubmissionTime() {
        return job.getSubmissionTime();
    }

    @Nullable
    @Override
    public String getName() {
        return job.getName();
    }

    @Nonnull
    @Override
    public JobStatus getStatus() {
        return job.getStatus();
    }

    @Override
    public boolean isUserCancelled() {
        return job.isUserCancelled();
    }

    @Override
    public UUID addStatusListener(@Nonnull JobStatusListener listener) {
        return job.addStatusListener(listener);
    }

    @Override
    public boolean removeStatusListener(@Nonnull UUID id) {
        return job.removeStatusListener(id);
    }

    @Nonnull
    @Override
    public JobConfig getConfig() {
        return job.getConfig();
    }

    @Override
    public JobConfig updateConfig(@Nonnull DeltaJobConfig deltaConfig) {
        return job.updateConfig(deltaConfig);
    }

    @Nonnull
    @Override
    public JobSuspensionCause getSuspensionCause() {
        return job.getSuspensionCause();
    }

    @Nonnull
    @Override
    public JobMetrics getMetrics() {
        return job.getMetrics();
    }

    @Override
    public void restart() {
        job.restart();
    }

    @Override
    public void suspend() {
        job.suspend();
    }

    @Override
    public void resume() {
        job.resume();
    }

    @Override
    public JobStateSnapshot cancelAndExportSnapshot(String name) {
        return job.cancelAndExportSnapshot(name);
    }

    @Override
    public JobStateSnapshot exportSnapshot(String name) {
        return job.exportSnapshot(name);
    }

    @Nonnull
    @Override
    public String getIdString() {
        return job.getIdString();
    }

    @Override
    public void join() {
        String message = "The job has started successfully. However the job should not call the join() method.\n"
                         + "Please remove the join() call";
        throw new JetException(message);
    }
}
