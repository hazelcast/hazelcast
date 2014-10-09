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

package com.hazelcast.concurrent.atomiclong.client;

import com.hazelcast.concurrent.atomiclong.operations.ApplyOperation;
import com.hazelcast.core.IFunction;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.spi.Operation;

import java.io.IOException;

public class ApplyRequest extends ReadRequest {

    private Data function;

    public ApplyRequest() {
    }

    public ApplyRequest(String name, Data function) {
        super(name);
        this.function = function;
    }

    @Override
    protected Operation prepareOperation() {
        IFunction f = serializationService.toObject(function);
        //noinspection unchecked
        return new ApplyOperation(name, f);
    }

    @Override
    public int getClassId() {
        return AtomicLongPortableHook.APPLY;
    }

    @Override
    public void write(PortableWriter writer) throws IOException {
        super.write(writer);
        ObjectDataOutput out = writer.getRawDataOutput();
        out.writeData(function);
    }

    @Override
    public void read(PortableReader reader) throws IOException {
        super.read(reader);
        ObjectDataInput in = reader.getRawDataInput();
        function = in.readData();
    }

    @Override
    public String getMethodName() {
        return "apply";
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{function};
    }
}
