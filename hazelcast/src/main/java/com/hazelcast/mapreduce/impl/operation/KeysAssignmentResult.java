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
import com.hazelcast.mapreduce.impl.operation.RequestPartitionResult.ResultState;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * This class is used to store assignment results in {@link com.hazelcast.mapreduce.impl.operation.KeysAssignmentOperation}
 * executions.
 */
public class KeysAssignmentResult
        implements IdentifiedDataSerializable {

    private ResultState resultState;
    private Map<Object, Address> assignment;

    public KeysAssignmentResult() {
    }

    public KeysAssignmentResult(ResultState resultState, Map<Object, Address> assignment) {
        this.resultState = resultState;
        this.assignment = assignment;
    }

    public ResultState getResultState() {
        return resultState;
    }

    public Map<Object, Address> getAssignment() {
        return assignment;
    }

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {
        out.writeBoolean(assignment != null);
        if (assignment != null) {
            out.writeInt(assignment.size());
            for (Map.Entry<Object, Address> entry : assignment.entrySet()) {
                out.writeObject(entry.getKey());
                out.writeObject(entry.getValue());
            }
        }
        out.writeInt(resultState.ordinal());
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        if (in.readBoolean()) {
            int size = in.readInt();
            assignment = new HashMap<Object, Address>(size);
            for (int i = 0; i < size; i++) {
                assignment.put(in.readObject(), (Address) in.readObject());
            }
        }
        resultState = ResultState.byOrdinal(in.readInt());
    }

    @Override
    public int getFactoryId() {
        return MapReduceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return MapReduceDataSerializerHook.KEYS_ASSIGNMENT_RESULT;
    }

}
