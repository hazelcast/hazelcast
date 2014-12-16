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

package com.hazelcast.concurrent.atomicreference.client;

import com.hazelcast.concurrent.atomicreference.operations.CompareAndSetOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.spi.Operation;

import java.io.IOException;

public class CompareAndSetRequest extends ModifyRequest {

    private Data expected;

    public CompareAndSetRequest() {
    }

    public CompareAndSetRequest(String name, Data expected, Data update) {
        super(name, update);
        this.expected = expected;
    }

    @Override
    protected Operation prepareOperation() {
        return new CompareAndSetOperation(name, expected, update);
    }

    @Override
    public int getClassId() {
        return AtomicReferencePortableHook.COMPARE_AND_SET;
    }

    @Override
    public void write(PortableWriter writer) throws IOException {
        super.write(writer);
        ObjectDataOutput out = writer.getRawDataOutput();
        out.writeData(expected);
    }

    @Override
    public void read(PortableReader reader) throws IOException {
        super.read(reader);
        ObjectDataInput in = reader.getRawDataInput();
        expected = in.readData();
    }

    @Override
    public String getMethodName() {
        return "compareAndSet";
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{expected, update};
    }
}
