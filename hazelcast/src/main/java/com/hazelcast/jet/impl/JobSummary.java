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

import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;

import static com.hazelcast.jet.Util.idToString;
import static com.hazelcast.jet.impl.util.Util.toLocalTime;

public class JobSummary implements IdentifiedDataSerializable {

    private boolean isLightJob;
    private long jobId;
    private long executionId;
    private String nameOrId;
    private JobStatus status;
    private long submissionTime;
    private long completionTime;
    private String failureText;

    public JobSummary() {
    }

    public JobSummary(
            boolean isLightJob,
            long jobId,
            long executionId,
            @Nonnull String nameOrId,
            @Nonnull JobStatus status,
            long submissionTime,
            long completionTime,
            String failureText,
            SqlSummary sqlSummary
    ) {
        this.isLightJob = isLightJob;
        this.jobId = jobId;
        this.executionId = executionId;
        this.nameOrId = nameOrId;
        this.status = status;
        this.submissionTime = submissionTime;
        this.completionTime = completionTime;
        this.failureText = failureText;
    }

    public boolean isLightJob() {
        return isLightJob;
    }

    public long getJobId() {
        return jobId;
    }

    /**
     * Returns execution id of the job if running, or 0 otherwise
     */
    public long getExecutionId() {
        return executionId;
    }

    /**
     * Return the job name (from jobConfig). If it doesn't have the name,
     * return the formatted job ID.
     */
    @Nonnull
    public String getNameOrId() {
        return nameOrId;
    }

    /**
     * @deprecated use {@link #getNameOrId()}, semantics is the same
     */
    @Nonnull
    @Deprecated
    public String getName() {
        return nameOrId;
    }

    @Nonnull
    public JobStatus getStatus() {
        return status;
    }

    public long getSubmissionTime() {
        return submissionTime;
    }

    /**
     * Returns 0 if job is not yet completed.
     */
    public long getCompletionTime() {
        return completionTime;
    }

    /**
     * Returns null if job is not yet completed.
     */
    @Nullable
    public String getFailureText() {
        return failureText;
    }

    @Override
    public int getFactoryId() {
        return JetInitDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return JetInitDataSerializerHook.JOB_SUMMARY;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeBoolean(isLightJob);
        out.writeLong(jobId);
        out.writeLong(executionId);
        out.writeString(nameOrId);
        out.writeObject(status);
        out.writeLong(submissionTime);
        out.writeLong(completionTime);
        out.writeString(failureText);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        isLightJob = in.readBoolean();
        jobId = in.readLong();
        executionId = in.readLong();
        nameOrId = in.readString();
        status = in.readObject();
        submissionTime = in.readLong();
        completionTime = in.readLong();
        failureText = in.readString();
    }

    @Override
    public String toString() {
        return "JobSummary{" +
                "jobId=" + idToString(jobId) +
                ", executionId=" + idToString(executionId) +
                ", name='" + nameOrId + '\'' +
                ", status=" + status +
                ", submissionTime=" + toLocalTime(submissionTime) +
                ", completionTime=" + toLocalTime(completionTime) +
                ", failureText=" + failureText +
                '}';
    }
}
