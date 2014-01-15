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
import com.hazelcast.mapreduce.impl.MapReducePortableHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;

public class TransferableJobProcessInformation
        implements JobProcessInformation, Portable {

    private JobPartitionState[] partitionStates;
    private int processedRecords;

    public TransferableJobProcessInformation() {
    }

    public TransferableJobProcessInformation(JobPartitionState[] partitionStates, int processedRecords) {
        this.partitionStates = partitionStates;
        this.processedRecords = processedRecords;
    }

    @Override
    public JobPartitionState[] getPartitionStates() {
        return partitionStates;
    }

    @Override
    public int getProcessedRecords() {
        return processedRecords;
    }

    @Override
    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeInt("processedRecords", processedRecords);
        ObjectDataOutput out = writer.getRawDataOutput();
        out.writeObject(partitionStates);
    }

    @Override
    public void readPortable(PortableReader reader) throws IOException {
        processedRecords = reader.readInt("processedRecords");
        ObjectDataInput in = reader.getRawDataInput();
        partitionStates = in.readObject();
    }

    @Override
    public int getFactoryId() {
        return MapReducePortableHook.F_ID;
    }

    @Override
    public int getClassId() {
        return MapReducePortableHook.TRANSFERABLE_PROCESS_INFORMATION;
    }

}
