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
import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;

// TODO RU_COMPAT This subclass exists so we can change the serialization of its superclass in an RU
//  compatible way when upgrading from versions using compact serialization (<= 5.6). Once RU from a version
//  <= 5.6 is no longer supported this class should be removed in favour of the superclass. The
//  IdentifiedDataSerializable methods in this class should be transferred there and all locations which create
//  instances of this class should be changed to create instances of the superclass.
public class JobAndSqlSummaryIds
        extends JobAndSqlSummary
        implements IdentifiedDataSerializable {

    @SuppressWarnings("checkstyle:parameternumber")
    public JobAndSqlSummaryIds(boolean isLightJob, long jobId, long executionId, @Nonnull String nameOrId,
                               @Nonnull JobStatus status, long submissionTime, long completionTime, String failureText,
                               SqlSummary sqlSummary, @Nullable String suspensionCause, boolean userCancelled) {
        super(isLightJob, jobId, executionId, nameOrId, status, submissionTime, completionTime, failureText, sqlSummary,
                suspensionCause, userCancelled);
    }

    public JobAndSqlSummaryIds() {
    }

    public JobAndSqlSummary toOldVersion() {
        return new JobAndSqlSummary(
                isLightJob,
                jobId,
                executionId,
                nameOrId,
                status,
                submissionTime,
                completionTime,
                failureText,
                sqlSummary,
                suspensionCause,
                userCancelled
        );
    }

    @Override
    public int getFactoryId() {
        return JetInitDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return JetInitDataSerializerHook.JOB_AND_SQL_SUMMARY;
    }

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {
        out.writeBoolean(isLightJob);
        out.writeLong(jobId);
        out.writeLong(executionId);
        out.writeString(nameOrId);
        out.writeObject(status);
        out.writeLong(submissionTime);
        out.writeLong(completionTime);
        out.writeString(failureText);
        boolean hasSummary = sqlSummary != null;
        out.writeBoolean(hasSummary);
        if (hasSummary) {
            out.writeString(sqlSummary.getQuery());
            out.writeBoolean(sqlSummary.isUnbounded());
        }
        out.writeString(suspensionCause);
        out.writeBoolean(userCancelled);
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        isLightJob = in.readBoolean();
        jobId = in.readLong();
        executionId = in.readLong();
        nameOrId = in.readString();
        status = in.readObject();
        submissionTime = in.readLong();
        completionTime = in.readLong();
        failureText = in.readString();
        boolean hasSummary = in.readBoolean();
        if (hasSummary) {
            sqlSummary = new SqlSummary(in.readString(), in.readBoolean());
        }
        suspensionCause = in.readString();
        userCancelled = in.readBoolean();
    }
}
