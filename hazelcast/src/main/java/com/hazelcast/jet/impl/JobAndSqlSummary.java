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

package com.hazelcast.jet.impl;

import com.hazelcast.jet.core.JobStatus;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;

public class JobAndSqlSummary {
    private final boolean isLightJob;
    private final long jobId;
    private final long executionId;
    private final String nameOrId;
    private final JobStatus status;
    private final long submissionTime;
    private final long completionTime;
    private final String failureText;
    private final SqlSummary sqlSummary;
    private final String suspensionCause;
    /**
     * True, if the job has been cancelled based on a user request, false
     * otherwise (also while the job is running).
     */
    private final boolean userCancelled;

    @SuppressWarnings("checkstyle:parameternumber")
    public JobAndSqlSummary(
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
        if (o == null || getClass() != o.getClass()) {
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
