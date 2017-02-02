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

package com.hazelcast.jet.impl.operation;

import com.hazelcast.jet.impl.JetService;
import com.hazelcast.jet.impl.execution.init.ExecutionPlan;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;

import java.io.IOException;
import java.util.function.Supplier;

import static com.hazelcast.jet.impl.execution.init.CustomClassLoadedObject.deserializeWithCustomClassLoader;

class InitOperation extends Operation {

    private long executionId;
    private Supplier<ExecutionPlan> planSupplier;

    InitOperation(long executionId, ExecutionPlan plan) {
        this.executionId = executionId;
        this.planSupplier = () -> plan;
    }

    private InitOperation() {
        // for deserialization
    }

    @Override
    public void run() throws Exception {
        getLogger().info("Initializing execution plan for job " + executionId + " from " + getCallerAddress() + ".");
        JetService service = getService();
        service.initExecution(executionId, planSupplier.get());
        getLogger().fine("Execution plan for job " + executionId + " initialized.");
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);

        out.writeLong(executionId);
        Data planBlob = getNodeEngine().getSerializationService().toData(planSupplier.get());
        out.writeData(planBlob);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        executionId = in.readLong();

        final Data planBlob = in.readData();
        planSupplier = () -> {
            JetService service = getService();
            ClassLoader cl = service.getClassLoader(executionId);
            return deserializeWithCustomClassLoader(getNodeEngine().getSerializationService(), cl, planBlob);
        };
    }
}
