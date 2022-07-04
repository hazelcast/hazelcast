/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.jet.JetException;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.jet.Util.idToString;
import static com.hazelcast.jet.core.JobStatus.COMPLETED;
import static com.hazelcast.jet.core.JobStatus.FAILED;
import static com.hazelcast.jet.impl.util.Util.exceptionallyCompletedFuture;
import static com.hazelcast.jet.impl.util.Util.toLocalDateTime;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class JobResult implements IdentifiedDataSerializable {

    private long jobId;
    private JobConfig jobConfig;
    private long creationTime;
    private long completionTime;
    private String failureText;

    public JobResult() {
    }

    JobResult(long jobId,
              @Nonnull JobConfig jobConfig,
              long creationTime,
              long completionTime,
              @Nullable String failureText
    ) {
        this.jobId = jobId;
        this.jobConfig = jobConfig;
        this.creationTime = creationTime;
        this.completionTime = completionTime;
        this.failureText = failureText;
    }

    public long getJobId() {
        return jobId;
    }

    @Nonnull
    public JobConfig getJobConfig() {
        return jobConfig;
    }

    public long getCreationTime() {
        return creationTime;
    }

    public long getCompletionTime() {
        return completionTime;
    }

    public boolean isSuccessful() {
        return failureText == null;
    }

    @Nullable
    public String getFailureText() {
        return failureText;
    }

    /**
     * Returns a mock throwable created for the failureText. It's either {@link
     * CancellationException} or {@link JetException} with the failureText
     * text.
     */
    @Nullable
    public Throwable getFailureAsThrowable() {
        if (failureText == null) {
            return null;
        }
        Throwable throwable;
        if (failureText.startsWith(CancellationException.class.getName())) {
            int prefixLength = (CancellationException.class.getName() + ": ").length();
            String message = failureText.length() >= prefixLength ? failureText.substring(prefixLength) : null;
            throwable = new CancellationException(message);
        } else {
            throwable = new JetException(failureText);
        }
        return throwable;
    }

    @Nonnull
    public JobStatus getJobStatus() {
        return isSuccessful() ? COMPLETED : FAILED;
    }

    @Nonnull
    CompletableFuture<Void> asCompletableFuture() {
        return failureText == null ? completedFuture(null) : exceptionallyCompletedFuture(getFailureAsThrowable());
    }

    @Nonnull
    public String getJobNameOrId() {
        return jobConfig.getName() != null ? jobConfig.getName() : idToString(jobId);
    }

    @Override
    public String toString() {
        return "JobResult{" +
                "jobId=" + idToString(jobId) +
                ", name=" + jobConfig.getName() +
                ", creationTime=" + toLocalDateTime(creationTime) +
                ", completionTime=" + toLocalDateTime(completionTime) +
                ", failureText=" + failureText +
                '}';
    }

    @Override
    public int getFactoryId() {
        return JetInitDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return JetInitDataSerializerHook.JOB_RESULT;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(jobId);
        out.writeObject(jobConfig);
        out.writeLong(creationTime);
        out.writeLong(completionTime);
        out.writeObject(failureText);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        jobId = in.readLong();
        jobConfig = in.readObject();
        creationTime = in.readLong();
        completionTime = in.readLong();
        failureText = in.readObject();
    }
}
