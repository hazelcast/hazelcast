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
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

public class RequestPartitionResult implements IdentifiedDataSerializable {

    private State state;
    private int partitionId;

    public RequestPartitionResult() {
    }

    public RequestPartitionResult(State state, int partitionId) {
        this.state = state;
        this.partitionId = partitionId;
    }

    public State getState() {
        return state;
    }

    public int getPartitionId() {
        return partitionId;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(state.ordinal());
        out.writeInt(partitionId);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        state = State.byOrdinal(in.readInt());
        partitionId = in.readInt();
    }

    @Override
    public int getFactoryId() {
        return MapReduceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return MapReduceDataSerializerHook.REQUEST_PARTITION_RESULT;
    }

    @Override
    public String toString() {
        return "RequestPartitionResult{" +
                "state=" + state +
                ", partitionId=" + partitionId +
                '}';
    }

    public static enum State {
        SUCCESSFUL,
        NO_SUPERVISOR,
        CHECK_STATE_FAILED,;

        public static State byOrdinal(int ordinal) {
            for (State state : values()) {
                if (ordinal == state.ordinal()) {
                    return state;
                }
            }
            return null;
        }
    }

}
