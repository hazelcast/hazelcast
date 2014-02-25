/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.mapreduce.impl.operation;

import com.hazelcast.mapreduce.JobPartitionState;
import com.hazelcast.mapreduce.impl.MapReduceDataSerializerHook;
import com.hazelcast.mapreduce.impl.MapReduceService;
import com.hazelcast.mapreduce.impl.task.JobProcessInformationImpl;
import com.hazelcast.mapreduce.impl.task.JobSupervisor;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

import static com.hazelcast.mapreduce.impl.operation.RequestPartitionResult.ResultState.CHECK_STATE_FAILED;
import static com.hazelcast.mapreduce.impl.operation.RequestPartitionResult.ResultState.NO_SUPERVISOR;
import static com.hazelcast.mapreduce.impl.operation.RequestPartitionResult.ResultState.SUCCESSFUL;

/**
 * This operation is used to tell the job owner to postpone a mapping phase for the defined
 * partitionId. This can happen if the partitionId might not yet be assigned to any cluster
 * members.
 */
public class PostPonePartitionProcessing
        extends ProcessingOperation {

    private volatile RequestPartitionResult result;

    private int partitionId;

    public PostPonePartitionProcessing() {
    }

    public PostPonePartitionProcessing(String name, String jobId, int partitionId) {
        super(name, jobId);
        this.partitionId = partitionId;
    }

    @Override
    public Object getResponse() {
        return result;
    }

    @Override
    public void run()
            throws Exception {
        MapReduceService mapReduceService = getService();
        JobSupervisor supervisor = mapReduceService.getJobSupervisor(getName(), getJobId());
        if (supervisor == null) {
            result = new RequestPartitionResult(NO_SUPERVISOR, -1);
            return;
        }

        JobProcessInformationImpl processInformation = supervisor.getJobProcessInformation();

        while (true) {
            JobPartitionState[] partitionStates = processInformation.getPartitionStates();
            JobPartitionState oldPartitionState = partitionStates[partitionId];

            if (oldPartitionState == null || !getCallerAddress().equals(oldPartitionState.getOwner())) {
                result = new RequestPartitionResult(CHECK_STATE_FAILED, partitionId);
                return;
            }

            if (processInformation.updatePartitionState(partitionId, oldPartitionState, null)) {
                result = new RequestPartitionResult(SUCCESSFUL, partitionId);
                return;
            }
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out)
            throws IOException {
        super.writeInternal(out);
        out.writeInt(partitionId);
    }

    @Override
    protected void readInternal(ObjectDataInput in)
            throws IOException {
        super.readInternal(in);
        partitionId = in.readInt();
    }

    @Override
    public int getFactoryId() {
        return MapReduceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return MapReduceDataSerializerHook.POSTPONE_PARTITION_PROCESSING_OPERATION;
    }
}
