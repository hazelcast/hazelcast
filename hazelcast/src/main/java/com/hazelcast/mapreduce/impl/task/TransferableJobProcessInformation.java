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
import com.hazelcast.mapreduce.JobProcessInformation;
import com.hazelcast.mapreduce.impl.MapReduceDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

public class TransferableJobProcessInformation
        implements JobProcessInformation, IdentifiedDataSerializable {

    private JobPartitionState[] partitionStates;
    private int processRecords;

    public TransferableJobProcessInformation() {
    }

    public TransferableJobProcessInformation(JobPartitionState[] partitionStates, int processRecords) {
        this.partitionStates = partitionStates;
        this.processRecords = processRecords;
    }

    @Override
    public JobPartitionState[] getPartitionStates() {
        return partitionStates;
    }

    @Override
    public int getProcessedRecords() {
        return processRecords;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(processRecords);
        out.writeObject(partitionStates);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        processRecords = in.readInt();
        partitionStates = in.readObject();
    }

    @Override
    public int getFactoryId() {
        return MapReduceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return MapReduceDataSerializerHook.TRANSFERABLE_PROCESS_INFORMATION;
    }

}
