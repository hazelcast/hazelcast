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
import com.hazelcast.mapreduce.JobProcessInformation;
import com.hazelcast.mapreduce.impl.MapReduceDataSerializerHook;
import com.hazelcast.mapreduce.impl.MapReduceService;
import com.hazelcast.mapreduce.impl.task.JobProcessInformationImpl;
import com.hazelcast.mapreduce.impl.task.JobSupervisor;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.List;

import static com.hazelcast.mapreduce.JobPartitionState.State.MAPPING;
import static com.hazelcast.mapreduce.JobPartitionState.State.WAITING;
import static com.hazelcast.mapreduce.impl.MapReduceUtil.stateChange;
import static com.hazelcast.mapreduce.impl.operation.RequestPartitionResult.ResultState.NO_MORE_PARTITIONS;
import static com.hazelcast.mapreduce.impl.operation.RequestPartitionResult.ResultState.NO_SUPERVISOR;
import static com.hazelcast.mapreduce.impl.operation.RequestPartitionResult.ResultState.SUCCESSFUL;

public class RequestPartitionMapping
        extends ProcessingOperation {

    private volatile RequestPartitionResult result;

    public RequestPartitionMapping() {
    }

    public RequestPartitionMapping(String name, String jobId) {
        super(name, jobId);
    }

    @Override
    public Object getResponse() {
        return result;
    }

    @Override
    public void run() throws Exception {
        MapReduceService mapReduceService = getService();
        JobSupervisor supervisor = mapReduceService.getJobSupervisor(getName(), getJobId());
        if (supervisor == null) {
            result = new RequestPartitionResult(NO_SUPERVISOR, -1);
            return;
        }

        List<Integer> memberPartitions = mapReduceService.getMemberPartitions(getCallerAddress());
        JobProcessInformationImpl processInformation = supervisor.getJobProcessInformation();

        for (; ; ) {
            int selectedPartition = searchMemberPartitionToProcess(processInformation, memberPartitions);
            if (selectedPartition == -1) {
                // All partitions seem to be assigned so give up
                result = new RequestPartitionResult(NO_MORE_PARTITIONS, -1);
                return;
            }

            JobPartitionState.State nextState = stateChange(getCallerAddress(), selectedPartition, WAITING,
                    processInformation, supervisor.getConfiguration());

            if (nextState == MAPPING) {
                result = new RequestPartitionResult(SUCCESSFUL, selectedPartition);
                return;
            }
        }
    }

    private int searchMemberPartitionToProcess(JobProcessInformation processInformation,
                                               List<Integer> memberPartitions) {
        for (int partitionId : memberPartitions) {
            if (checkState(processInformation, partitionId)) {
                return partitionId;
            }
        }
        return -1;
    }

    private boolean checkState(JobProcessInformation processInformation, int partitionId) {
        JobPartitionState[] partitionStates = processInformation.getPartitionStates();
        JobPartitionState partitionState = partitionStates[partitionId];
        if (partitionState == null) {
            return true;
        }
        if (partitionState.getState() == JobPartitionState.State.WAITING) {
            return true;
        }
        return false;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
    }

    @Override
    public int getFactoryId() {
        return MapReduceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return MapReduceDataSerializerHook.REQUEST_PARTITION_MAPPING;
    }

}
