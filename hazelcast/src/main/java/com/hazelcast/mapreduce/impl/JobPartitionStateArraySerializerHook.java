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

package com.hazelcast.mapreduce.impl;

import com.hazelcast.mapreduce.JobPartitionState;
import com.hazelcast.mapreduce.impl.task.JobPartitionStateImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.SerializationConstants;
import com.hazelcast.nio.serialization.Serializer;
import com.hazelcast.nio.serialization.SerializerHook;
import com.hazelcast.nio.serialization.StreamSerializer;

import java.io.IOException;

/**
 * This class is the auto registered serializer hook for a {@link com.hazelcast.mapreduce.JobPartitionState} array.
 */
public class JobPartitionStateArraySerializerHook
        implements SerializerHook<JobPartitionState[]> {

    @Override
    public Class<JobPartitionState[]> getSerializationType() {
        return JobPartitionState[].class;
    }

    @Override
    public Serializer createSerializer() {
        return new JobPartitionStateArraySerializer();
    }

    @Override
    public boolean isOverwritable() {
        return false;
    }

    /**
     * The {@link com.hazelcast.mapreduce.JobPartitionState} array serializer itself
     */
    private static class JobPartitionStateArraySerializer
            implements StreamSerializer<JobPartitionState[]> {

        @Override
        public void write(ObjectDataOutput out, JobPartitionState[] partitionStates)
                throws IOException {
            out.writeBoolean(partitionStates != null);
            if (partitionStates != null) {
                out.writeInt(partitionStates.length);
                for (JobPartitionState partitionState : partitionStates) {
                    out.writeBoolean(partitionState != null);
                    if (partitionState != null) {
                        out.writeObject(partitionState.getOwner());
                        out.writeInt(partitionState.getState().ordinal());
                    }
                }
            }
        }

        @Override
        public JobPartitionState[] read(ObjectDataInput in)
                throws IOException {
            if (in.readBoolean()) {
                int length = in.readInt();
                JobPartitionState[] partitionStates = new JobPartitionState[length];
                for (int i = 0; i < length; i++) {
                    if (in.readBoolean()) {
                        Address owner = in.readObject();
                        JobPartitionState.State state = JobPartitionState.State.byOrdinal(in.readInt());
                        partitionStates[i] = new JobPartitionStateImpl(owner, state);
                    }
                }
                return partitionStates;
            }
            return null;
        }

        @Override
        public int getTypeId() {
            return SerializationConstants.AUTO_TYPE_JOB_PARTITION_STATE_ARRAY;
        }

        @Override
        public void destroy() {
        }
    }

}
