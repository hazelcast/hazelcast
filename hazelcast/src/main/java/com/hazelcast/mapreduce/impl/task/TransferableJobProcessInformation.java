/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;

/**
 * This implementation of {@link com.hazelcast.mapreduce.JobProcessInformation} is used to
 * transmit the currently processed number of records and the partition states to a requesting
 * client. This information can only be requested by the job emitting client and by requesting
 * it at the job owner.
 */
public class TransferableJobProcessInformation
        implements JobProcessInformation, Portable {

    private JobPartitionState[] partitionStates;
    private int processedRecords;

    public TransferableJobProcessInformation() {
    }

    public TransferableJobProcessInformation(JobPartitionState[] partitionStates, int processedRecords) {
        this.partitionStates = new JobPartitionState[partitionStates.length];
        System.arraycopy(partitionStates, 0, this.partitionStates, 0, partitionStates.length);
        this.processedRecords = processedRecords;
    }

    @Override
    // This field is explicitly exposed since it is guarded by a serialization cycle
    // or by a copy inside the constructor. This class is only used for transfer of
    // the states and user can change it without breaking anything.
    @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "exposed since it is guarded by serialization cycle"
            + " or by copy inside the constructor. This class is only used for transfer of"
            + " the states and user can change it without breaking anything.")
    public JobPartitionState[] getPartitionStates() {
        return partitionStates;
    }

    @Override
    public int getProcessedRecords() {
        return processedRecords;
    }

    @Override
    public void writePortable(PortableWriter writer)
            throws IOException {
        writer.writeInt("processedRecords", processedRecords);
        ObjectDataOutput out = writer.getRawDataOutput();
        out.writeObject(partitionStates);
    }

    @Override
    public void readPortable(PortableReader reader)
            throws IOException {
        processedRecords = reader.readInt("processedRecords");
        ObjectDataInput in = reader.getRawDataInput();
        partitionStates = in.readObject();
    }

    @Override
    public int getFactoryId() {
        return 1;
    }

    @Override
    public int getClassId() {
        return 2;
    }

}
