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

import javax.annotation.Nonnull;

public class JobAndSqlSummary {
    private boolean isLightJob;
    private long jobId;
    private long executionId;
    private String nameOrId;
    private JobStatus status;
    private long submissionTime;
    private long completionTime;
    private String failureText;
    private SqlSummary sqlSummary;

    public JobAndSqlSummary() {
    }

    public JobAndSqlSummary(
            boolean isLightJob,
            long jobId,
            long executionId,
            @Nonnull String nameOrId,
            @Nonnull JobStatus status,
            long submissionTime,
            long completionTime,
            String failureText,
            SqlSummary sqlSummary) {
        this.isLightJob = isLightJob;
        this.jobId = jobId;
        this.executionId = executionId;
        this.nameOrId = nameOrId;
        this.status = status;
        this.submissionTime = submissionTime;
        this.completionTime = completionTime;
        this.failureText = failureText;
        this.sqlSummary = sqlSummary;
    }

    public boolean isLightJob() {
        return isLightJob;
    }

    public void setLightJob(boolean lightJob) {
        isLightJob = lightJob;
    }

    public long getJobId() {
        return jobId;
    }

    public void setJobId(long jobId) {
        this.jobId = jobId;
    }

    public long getExecutionId() {
        return executionId;
    }

    public void setExecutionId(long executionId) {
        this.executionId = executionId;
    }

    public String getNameOrId() {
        return nameOrId;
    }

    public void setNameOrId(String nameOrId) {
        this.nameOrId = nameOrId;
    }

    public JobStatus getStatus() {
        return status;
    }

    public void setStatus(JobStatus status) {
        this.status = status;
    }

    public long getSubmissionTime() {
        return submissionTime;
    }

    public void setSubmissionTime(long submissionTime) {
        this.submissionTime = submissionTime;
    }

    public long getCompletionTime() {
        return completionTime;
    }

    public void setCompletionTime(long completionTime) {
        this.completionTime = completionTime;
    }

    public String getFailureText() {
        return failureText;
    }

    public void setFailureText(String failureText) {
        this.failureText = failureText;
    }

    public SqlSummary getSqlSummary() {
        return sqlSummary;
    }

    public void setSqlSummary(SqlSummary sqlSummary) {
        this.sqlSummary = sqlSummary;
    }

    @Override
    public String toString() {
        return "JobAndSqlSummary{" +
                "isLightJob=" + isLightJob +
                ", jobId=" + jobId +
                ", executionId=" + executionId +
                ", nameOrId='" + nameOrId + '\'' +
                ", status=" + status +
                ", submissionTime=" + submissionTime +
                ", completionTime=" + completionTime +
                ", failureText='" + failureText + '\'' +
                ", sqlSummary=" + sqlSummary +
                '}';
    }
}
