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

package com.hazelcast.concurrent.atomiclong.operations;

import com.hazelcast.concurrent.atomiclong.AtomicLongDataSerializerHook;
import com.hazelcast.concurrent.atomiclong.LongWrapper;
import com.hazelcast.core.IFunction;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

public class ApplyOperation<R> extends AtomicLongBaseOperation {

    private IFunction<Long, R> function;
    private R returnValue;

    public ApplyOperation() {
    }

    public ApplyOperation(String name, IFunction<Long, R> function) {
        super(name);
        this.function = function;
    }

    @Override
    public void run() throws Exception {
        LongWrapper number = getNumber();
        returnValue = function.apply(number.get());
    }

    @Override
    public R getResponse() {
        return returnValue;
    }


    @Override
    public int getId() {
        return AtomicLongDataSerializerHook.APPLY;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(function);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        function = in.readObject();
    }
}
