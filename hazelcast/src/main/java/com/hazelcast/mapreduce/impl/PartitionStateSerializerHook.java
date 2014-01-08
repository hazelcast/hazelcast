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

public class PartitionStateSerializerHook implements SerializerHook<JobPartitionStateImpl> {

    @Override
    public Class<JobPartitionStateImpl> getSerializationType() {
        return JobPartitionStateImpl.class;
    }

    @Override
    public Serializer createSerializer() {
        return new JobPartitionStateSerializer();
    }

    @Override
    public boolean isOverwritable() {
        return false;
    }

    private static class JobPartitionStateSerializer implements StreamSerializer<JobPartitionStateImpl> {

        @Override
        public void write(ObjectDataOutput out, JobPartitionStateImpl object) throws IOException {
            out.writeObject(object.getOwner());
            out.writeObject(object.getState());
        }

        @Override
        public JobPartitionStateImpl read(ObjectDataInput in) throws IOException {
            Address owner = in.readObject();
            JobPartitionState.State state = in.readObject();
            return new JobPartitionStateImpl(owner, state);
        }

        @Override
        public int getTypeId() {
            return SerializationConstants.AUTO_TYPE_JOB_PARTITION_STATE;
        }

        @Override
        public void destroy() {
        }
    }

}
