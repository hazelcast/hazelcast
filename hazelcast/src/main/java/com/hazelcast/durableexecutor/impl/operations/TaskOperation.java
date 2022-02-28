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

package com.hazelcast.durableexecutor.impl.operations;

import com.hazelcast.durableexecutor.impl.DurableExecutorContainer;
import com.hazelcast.durableexecutor.impl.DurableExecutorDataSerializerHook;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.operationservice.BackupAwareOperation;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.MutatingOperation;

import java.io.IOException;
import java.util.concurrent.Callable;

public class TaskOperation extends AbstractDurableExecutorOperation implements BackupAwareOperation, MutatingOperation {

    private Data callableData;
    private transient int sequence;
    private transient Callable callable;

    public TaskOperation() {
    }

    public TaskOperation(String name, Data callableData) {
        super(name);
        this.callableData = callableData;
    }

    @Override
    public void run() throws Exception {
        callable = getNodeEngine().toObject(callableData);
        DurableExecutorContainer executorContainer = getExecutorContainer();
        sequence = executorContainer.execute(callable);
    }

    @Override
    public Object getResponse() {
        return sequence;
    }

    @Override
    public Operation getBackupOperation() {
        return new TaskBackupOperation(name, sequence, callableData);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        IOUtil.writeData(out, callableData);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        callableData = IOUtil.readData(in);
    }

    @Override
    public int getClassId() {
        return DurableExecutorDataSerializerHook.TASK;
    }
}
