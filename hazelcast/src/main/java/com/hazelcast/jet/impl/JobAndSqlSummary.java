/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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
import javax.annotation.Nullable;
import java.util.Objects;

public class JobAndSqlSummary {
    protected boolean isLightJob;
    protected long jobId;
    protected long executionId;
    protected String nameOrId;
    protected JobStatus status;
    protected long submissionTime;
    protected long completionTime;
    protected String failureText;
    protected SqlSummary sqlSummary;
    protected String suspensionCause;
    /**
     * True, if the job has been cancelled based on a user request, false
     * otherwise (also while the job is running).
     */
    protected boolean userCancelled;

    public JobAndSqlSummary() {
    }

    @SuppressWarnings("checkstyle:parameternumber")
    protected JobAndSqlSummary(
            boolean isLightJob,
            long jobId,
            long executionId,
            @Nonnull String nameOrId,
            @Nonnull JobStatus status,
            long submissionTime,
            long completionTime,
            String failureText,
            SqlSummary sqlSummary,
            @Nullable String suspensionCause,
            boolean userCancelled) {
        this.isLightJob = isLightJob;
        this.jobId = jobId;
        this.executionId = executionId;
        this.nameOrId = nameOrId;
        this.status = status;
        this.submissionTime = submissionTime;
        this.completionTime = completionTime;
        this.failureText = failureText;
        this.sqlSummary = sqlSummary;
        this.suspensionCause = suspensionCause;
        this.userCancelled = userCancelled;
    }

    public boolean isLightJob() {
        return isLightJob;
    }

    public long getJobId() {
        return jobId;
    }

    public long getExecutionId() {
        return executionId;
    }

    public String getNameOrId() {
        return nameOrId;
    }

    public JobStatus getStatus() {
        return status;
    }

    public long getSubmissionTime() {
        return submissionTime;
    }

    public long getCompletionTime() {
        return completionTime;
    }

    public String getFailureText() {
        return failureText;
    }

    public SqlSummary getSqlSummary() {
        return sqlSummary;
    }

    public String getSuspensionCause() {
        return suspensionCause;
    }

    /**
     * @return true, if the job has been cancelled based on a user request,
     * false otherwise (also while the job is running).
     */
    public boolean isUserCancelled() {
        return userCancelled;
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
                ", suspensionCause=" + suspensionCause +
                ", userCancelled=" + userCancelled +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || !JobAndSqlSummary.class.isAssignableFrom(o.getClass())) {
            return false;
        }
        JobAndSqlSummary that = (JobAndSqlSummary) o;
        boolean suspensionCauseEquals = true;
        if (suspensionCause != null && that.suspensionCause != null) {
            suspensionCauseEquals = Objects.equals(suspensionCause, that.suspensionCause);
        }

        return isLightJob == that.isLightJob && jobId == that.jobId && executionId == that.executionId
                && submissionTime == that.submissionTime && completionTime == that.completionTime
                && Objects.equals(nameOrId, that.nameOrId) && status == that.status
                && Objects.equals(failureText, that.failureText)
                && Objects.equals(sqlSummary, that.sqlSummary)
                && suspensionCauseEquals
                && userCancelled == that.userCancelled;
    }

    @Override
    public int hashCode() {
        return Objects.hash(isLightJob, jobId, executionId, nameOrId, status, submissionTime,
                completionTime, failureText, sqlSummary, suspensionCause, userCancelled);
    }
}
