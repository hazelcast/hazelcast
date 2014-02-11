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

/**
 * This class is used to store the requested result of all kinds of processing operations.<br/>
 * By default it holds a basic result state and maybe the partitionId it was operated otherwise
 * it's value is defined as -1
 */
public class RequestPartitionResult
        implements IdentifiedDataSerializable {

    private ResultState resultState;
    private int partitionId;

    public RequestPartitionResult() {
    }

    public RequestPartitionResult(ResultState resultState, int partitionId) {
        this.resultState = resultState;
        this.partitionId = partitionId;
    }

    public ResultState getResultState() {
        return resultState;
    }

    public int getPartitionId() {
        return partitionId;
    }

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {
        out.writeInt(resultState.ordinal());
        out.writeInt(partitionId);
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        resultState = ResultState.byOrdinal(in.readInt());
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
        return "RequestPartitionResult{" + "resultState=" + resultState + ", partitionId=" + partitionId + '}';
    }

    /**
     * This enum is used to define the basic state of an operations result
     */
    public static enum ResultState {
        /**
         * Operation was successfully executed, partitionId contains value other than -1
         */
        SUCCESSFUL,

        /**
         * Operation wasn't executed, because no supervisor could be found for the given
         * name-jobId combination, partitionId is -1
         */
        NO_SUPERVISOR,

        /**
         * Operation wasn't executed since the old partition owner is not the requesting member,
         * value of partitionId is undefined (depending on the operation)
         */
        CHECK_STATE_FAILED,

        /**
         * Operation was executed but no more partitions seem to be available for mapping,
         * partitionId value is -1
         */
        NO_MORE_PARTITIONS;

        public static ResultState byOrdinal(int ordinal) {
            for (ResultState resultState : values()) {
                if (ordinal == resultState.ordinal()) {
                    return resultState;
                }
            }
            return null;
        }
    }

}
