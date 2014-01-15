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

package com.hazelcast.mapreduce.impl.task;

import com.hazelcast.mapreduce.JobPartitionState;
import com.hazelcast.nio.Address;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.mapreduce.JobPartitionState.State.WAITING;
import static com.hazelcast.mapreduce.impl.MapReduceUtil.stateChange;

public class MemberAssigningJobProcessInformationImpl extends JobProcessInformationImpl {

    private final ConcurrentMap<String, Integer> memberIds = new ConcurrentHashMap<String, Integer>();

    public MemberAssigningJobProcessInformationImpl(int partitionCount, JobSupervisor supervisor) {
        super(partitionCount, supervisor);
    }

    public int assignMemberId(Address address, String memberUuid, JobTaskConfiguration configuration) {
        JobPartitionState[] partitionStates = getPartitionStates();
        for (int i = 0; i < partitionStates.length; i++) {
            JobPartitionState partitionState = partitionStates[i];
            if (partitionState == null
                    || partitionState.getState() == JobPartitionState.State.WAITING) {

                // Seems unassigned so let try to use it
                if (stateChange(address, i, WAITING, this, configuration) != null) {
                    memberIds.put(memberUuid, i);
                    return i;
                }
            }
        }
        return -1;
    }

}
