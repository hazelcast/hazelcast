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

package com.hazelcast.spi.impl.operationservice;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.SpiDataSerializerHook;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;

/**
 * Content of an Operation Control packet:
 * <ol>
 * <li>a list of operations the remote member started on this member which are still running;</li>
 * <li>a list of operations this member wants to cancel on the remote member.</li>
 * </ol>
 * Operations are identified by their call ID.
 */
@SuppressFBWarnings(value = "EI", justification =
        "The priority is minimizing garbage. The caller guarantees not to mutate the long[] arrays.")
public final class OperationControl implements IdentifiedDataSerializable {

    private long[] runningOperations;
    private long[] operationsToCancel;

    public OperationControl() {
    }

    @SuppressFBWarnings("EI_EXPOSE_REP2")
    public OperationControl(long[] runningOperations, long[] operationsToCancel) {
        this.runningOperations = runningOperations;
        this.operationsToCancel = operationsToCancel;
    }

    @SuppressFBWarnings("EI_EXPOSE_REP")
    public long[] runningOperations() {
        return runningOperations;
    }

    @SuppressFBWarnings("EI_EXPOSE_REP")
    public long[] operationsToCancel() {
        return operationsToCancel;
    }

    @Override
    public int getFactoryId() {
        return SpiDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SpiDataSerializerHook.OPERATION_CONTROL;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLongArray(runningOperations);
        out.writeLongArray(operationsToCancel);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        runningOperations = in.readLongArray();
        operationsToCancel = in.readLongArray();
    }
}
