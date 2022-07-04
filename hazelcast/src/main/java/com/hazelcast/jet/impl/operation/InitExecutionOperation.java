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

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.cluster.MemberInfo;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.impl.JetServiceBackend;
import com.hazelcast.jet.impl.JobClassLoaderService;
import com.hazelcast.jet.impl.execution.init.ExecutionPlan;
import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.jet.impl.util.LoggingUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.version.Version;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.jet.impl.JobClassLoaderService.JobPhase.EXECUTION;
import static com.hazelcast.jet.impl.execution.init.CustomClassLoadedObject.deserializeWithCustomClassLoader;
import static com.hazelcast.jet.impl.util.Util.jobIdAndExecutionId;

/**
 * Operation sent from master to members to initialize execution of a job.
 * The behavior is different for light and normal jobs:
 * <ul>
 *     <li>for light jobs, it immediately starts the execution
 *     <li>for normal jobs, after the master receives all responses to this op,
 *         it sends {@link StartExecutionOperation}.
 * </ul>
 */
public class InitExecutionOperation extends AsyncJobOperation {

    private long executionId;
    private int coordinatorMemberListVersion;
    private Version coordinatorVersion;
    private Set<MemberInfo> participants;
    private Data serializedPlan;
    private boolean isLightJob;

    public InitExecutionOperation() {
    }

    public InitExecutionOperation(long jobId, long executionId, int coordinatorMemberListVersion,
                                  Version coordinatorVersion,
                                  Set<MemberInfo> participants, Data serializedPlan, boolean isLightJob) {
        super(jobId);
        this.executionId = executionId;
        this.coordinatorMemberListVersion = coordinatorMemberListVersion;
        this.coordinatorVersion = coordinatorVersion;
        this.participants = participants;
        this.serializedPlan = serializedPlan;
        this.isLightJob = isLightJob;
    }

    @Override
    protected CompletableFuture<?> doRun() {
        ILogger logger = getLogger();
        if (!getNodeEngine().getLocalMember().getVersion().asVersion().equals(coordinatorVersion)) {
            // Operations are sent to targets by Address. It can happen that the coordinator finds members
            // with the same version, but some member is upgraded before the operation is sent and has
            // the same address.
            throw new JetException("Mismatch between coordinator and participant version");
        }

        JetServiceBackend service = getJetServiceBackend();
        Address caller = getCallerAddress();
        LoggingUtil.logFine(logger, "Initializing execution plan for %s from %s", jobIdAndExecutionId(jobId(), executionId),
                caller);

        ExecutionPlan plan = deserializePlan(serializedPlan);
        if (isLightJob) {
            return service.getJobExecutionService().runLightJob(jobId(), executionId, caller,
                    coordinatorMemberListVersion, participants, plan);
        } else {
            service.getJobExecutionService().initExecution(jobId(), executionId, caller,
                    coordinatorMemberListVersion, participants, plan);
            return CompletableFuture.completedFuture(null);
        }
    }

    @Override
    public int getClassId() {
        return JetInitDataSerializerHook.INIT_EXECUTION_OP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);

        out.writeLong(executionId);
        out.writeBoolean(isLightJob);
        out.writeInt(coordinatorMemberListVersion);
        out.writeObject(coordinatorVersion);
        out.writeInt(participants.size());
        for (MemberInfo participant : participants) {
            out.writeObject(participant);
        }
        IOUtil.writeData(out, serializedPlan);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);

        executionId = in.readLong();
        isLightJob = in.readBoolean();
        coordinatorMemberListVersion = in.readInt();
        coordinatorVersion = in.readObject();
        int count = in.readInt();
        participants = new HashSet<>();
        for (int i = 0; i < count; i++) {
            participants.add(in.readObject());
        }
        serializedPlan = IOUtil.readData(in);
    }

    private ExecutionPlan deserializePlan(Data planBlob) {
        if (isLightJob) {
            return getNodeEngine().getSerializationService().toObject(planBlob);
        } else {
            JetServiceBackend service = getJetServiceBackend();
            JobConfig jobConfig = service.getJobConfig(jobId(), isLightJob);
            JobClassLoaderService jobClassloaderService = service.getJobClassLoaderService();

            ClassLoader cl = jobClassloaderService.getOrCreateClassLoader(jobConfig, jobId(), EXECUTION);
            try {
                jobClassloaderService.prepareProcessorClassLoaders(jobId());
                return deserializeWithCustomClassLoader(getNodeEngine().getSerializationService(), cl, planBlob);
            } finally {
                jobClassloaderService.clearProcessorClassLoaders();
            }
        }
    }
}
