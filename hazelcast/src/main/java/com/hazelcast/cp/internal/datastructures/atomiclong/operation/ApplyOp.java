/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal.datastructures.atomiclong.operation;

import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IFunction;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.datastructures.atomiclong.RaftAtomicLongDataSerializerHook;
import com.hazelcast.cp.internal.datastructures.atomiclong.RaftAtomicLong;

import java.io.IOException;

/**
 * Operation for {@link IAtomicLong#apply(IFunction)}
 *
 * @param <R> return type of the applied function
 */
public class ApplyOp<R> extends AbstractAtomicLongOp {

    private IFunction<Long, R> function;

    public ApplyOp() {
    }

    public ApplyOp(String name, IFunction<Long, R> function) {
        super(name);
        this.function = function;
    }

    @Override
    public int getClassId() {
        return RaftAtomicLongDataSerializerHook.APPLY_OP;
    }

    @Override
    public Object run(CPGroupId groupId, long commitIndex) {
        RaftAtomicLong atomic = getAtomicLong(groupId);
        long val = atomic.getAndAdd(0);
        return function.apply(val);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeObject(function);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        function = in.readObject();
    }

}
