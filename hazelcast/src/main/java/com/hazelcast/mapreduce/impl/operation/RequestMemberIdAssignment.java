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

import com.hazelcast.mapreduce.impl.MapReduceDataSerializerHook;
import com.hazelcast.mapreduce.impl.MapReduceService;
import com.hazelcast.mapreduce.impl.task.JobSupervisor;
import com.hazelcast.mapreduce.impl.task.MemberAssigningJobProcessInformationImpl;

import static com.hazelcast.mapreduce.impl.operation.RequestPartitionResult.ResultState.NO_MORE_PARTITIONS;
import static com.hazelcast.mapreduce.impl.operation.RequestPartitionResult.ResultState.NO_SUPERVISOR;
import static com.hazelcast.mapreduce.impl.operation.RequestPartitionResult.ResultState.SUCCESSFUL;

/**
 * This operation is used to do some kind of partitionId based processing on non partition based implementations
 * of {@link com.hazelcast.mapreduce.KeyValueSource} (not implementing {@link com.hazelcast.mapreduce.PartitionIdAware})
 * which can happen for custom data sources like distributed filesystems that are up to the end user on how to
 * manage the distribution.
 */
public class RequestMemberIdAssignment
        extends ProcessingOperation {

    private volatile RequestPartitionResult result;

    public RequestMemberIdAssignment() {
    }

    public RequestMemberIdAssignment(String name, String jobId) {
        super(name, jobId);
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

        MemberAssigningJobProcessInformationImpl processInformation = getProcessInformation(supervisor);
        int memberId = processInformation.assignMemberId(getCallerAddress(), getCallerUuid(), supervisor.getConfiguration());

        if (memberId == -1) {
            result = new RequestPartitionResult(NO_MORE_PARTITIONS, -1);
            return;
        }

        result = new RequestPartitionResult(SUCCESSFUL, memberId);
    }

    @Override
    public int getFactoryId() {
        return MapReduceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return MapReduceDataSerializerHook.REQUEST_MEMBERID_ASSIGNMENT;
    }

    private MemberAssigningJobProcessInformationImpl getProcessInformation(JobSupervisor supervisor) {
        return (MemberAssigningJobProcessInformationImpl) supervisor.getJobProcessInformation();
    }

}
