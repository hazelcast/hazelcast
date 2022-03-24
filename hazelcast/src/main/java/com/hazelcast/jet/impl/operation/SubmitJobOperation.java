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

package com.hazelcast.jet.impl.operation;

import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import javax.security.auth.Subject;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public class SubmitJobOperation extends AsyncJobOperation {
    private transient Object deserializedJobDefinition;
    private transient JobConfig deserializedJobConfig;

    // force serialization of fields to avoid sharing of the mutable instances if submitted to the master member
    private Data serializedJobDefinition;
    private Data serializedJobConfig;
    private boolean isLightJob;
    private Subject subject;

    public SubmitJobOperation() {
    }

    public SubmitJobOperation(
            long jobId,
            Object deserializedJobDefinition,
            JobConfig deserializedJobConfig,
            Data serializedJobDefinition,
            Data serializedJobConfig,
            boolean isLightJob,
            Subject subject
    ) {
        super(jobId);
        this.deserializedJobDefinition = deserializedJobDefinition;
        this.deserializedJobConfig = deserializedJobConfig;
        this.serializedJobDefinition = serializedJobDefinition;
        this.serializedJobConfig = serializedJobConfig;
        this.isLightJob = isLightJob;
        this.subject = subject;
    }

    @Override
    public CompletableFuture<Void> doRun() {
        JobConfig jobConfig = deserializedJobConfig != null ? deserializedJobConfig :
                getNodeEngine().getSerializationService().toObject(serializedJobConfig);
        if (isLightJob) {
            if (deserializedJobDefinition != null) {
                return getJobCoordinationService().submitLightJob(jobId(), deserializedJobDefinition, null, jobConfig, subject);
            }
            return getJobCoordinationService().submitLightJob(jobId(), null, serializedJobDefinition, jobConfig, subject);
        }
        assert deserializedJobDefinition == null; // the jobDefinition for non-light job is always serialized
        return getJobCoordinationService().submitJob(jobId(), serializedJobDefinition, jobConfig, subject);
    }

    @Override
    public int getClassId() {
        return JetInitDataSerializerHook.SUBMIT_JOB_OP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);

        assert serializedJobDefinition != null;
        assert serializedJobConfig != null;
        assert deserializedJobDefinition == null;
        assert deserializedJobConfig == null;

        IOUtil.writeData(out, serializedJobDefinition);
        IOUtil.writeData(out, serializedJobConfig);
        out.writeBoolean(isLightJob);
        out.writeObject(subject);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        serializedJobDefinition = IOUtil.readData(in);
        serializedJobConfig = IOUtil.readData(in);
        isLightJob = in.readBoolean();
        subject = in.readObject();
    }
}
