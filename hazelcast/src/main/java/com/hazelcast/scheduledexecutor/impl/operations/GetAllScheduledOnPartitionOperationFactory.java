/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.scheduledexecutor.impl.operations;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.scheduledexecutor.impl.ScheduledExecutorDataSerializerHook;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationFactory;

import java.io.IOException;

public class GetAllScheduledOnPartitionOperationFactory
        implements OperationFactory {

    private String schedulerName;

    public GetAllScheduledOnPartitionOperationFactory() {
    }

    public GetAllScheduledOnPartitionOperationFactory(String schedulerName) {
        this.schedulerName = schedulerName;
    }

    @Override
    public int getFactoryId() {
        return ScheduledExecutorDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ScheduledExecutorDataSerializerHook.GET_ALL_SCHEDULED_ON_PARTITION_OPERATION_FACTORY;
    }

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {
        out.writeString(schedulerName);
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        schedulerName = in.readString();
    }

    @Override
    public Operation createOperation() {
        return new GetAllScheduledOnPartitionOperation(schedulerName);
    }
}
