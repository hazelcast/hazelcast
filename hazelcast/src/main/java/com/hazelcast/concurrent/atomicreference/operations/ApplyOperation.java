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

package com.hazelcast.concurrent.atomicreference.operations;

import com.hazelcast.concurrent.atomicreference.AtomicReferenceContainer;
import com.hazelcast.core.IFunction;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.MutatingOperation;

import java.io.IOException;

import static com.hazelcast.concurrent.atomicreference.AtomicReferenceDataSerializerHook.APPLY;

public class ApplyOperation extends AbstractAtomicReferenceOperation implements MutatingOperation {

    protected Data function;
    protected Data returnValue;

    public ApplyOperation() {
    }

    public ApplyOperation(String name, Data function) {
        super(name);
        this.function = function;
    }

    @Override
    public void run() throws Exception {
        NodeEngine nodeEngine = getNodeEngine();
        IFunction f = nodeEngine.toObject(function);
        AtomicReferenceContainer container = getReferenceContainer();

        Object input = nodeEngine.toObject(container.get());
        //noinspection unchecked
        Object output = f.apply(input);
        returnValue = nodeEngine.toData(output);
    }

    @Override
    public Object getResponse() {
        return returnValue;
    }

    @Override
    public int getId() {
        return APPLY;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeData(function);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        function = in.readData();
    }
}
