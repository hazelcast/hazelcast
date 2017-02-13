/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.BinaryInterface;

import java.io.IOException;

/**
 * This class holds information about the current processing state and the owner of a partition.
 */
@BinaryInterface
public class JobPartitionStateImpl implements JobPartitionState, DataSerializable {

    private Address address;
    private State state;

    public JobPartitionStateImpl(Address address, State state) {
        this.address = address;
        this.state = state;
    }

    @Override
    public Address getOwner() {
        return address;
    }

    @Override
    public State getState() {
        return state;
    }

    @Override
    public String toString() {
        return "JobPartitionStateImpl{" + "state=" + state + ", address=" + address + '}';
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(address);
        out.writeInt(state.ordinal());
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        address = in.readObject();
        state = JobPartitionState.State.byOrdinal(in.readInt());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        JobPartitionStateImpl that = (JobPartitionStateImpl) o;

        if (address != null ? !address.equals(that.address) : that.address != null) {
            return false;
        }
        return state == that.state;

    }

    @Override
    public int hashCode() {
        int result = address != null ? address.hashCode() : 0;
        result = 31 * result + (state != null ? state.hashCode() : 0);
        return result;
    }
}
