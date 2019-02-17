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

package com.hazelcast.mapreduce.impl.operation;

import com.hazelcast.mapreduce.impl.MapReduceDataSerializerHook;
import com.hazelcast.mapreduce.impl.operation.RequestPartitionResult.ResultState;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.Map;

import static com.hazelcast.internal.serialization.impl.SerializationUtil.readNullableMap;
import static com.hazelcast.internal.serialization.impl.SerializationUtil.writeNullableMap;

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
        writeNullableMap(assignment, out);
        out.writeInt(resultState.ordinal());
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        assignment = readNullableMap(in);
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
