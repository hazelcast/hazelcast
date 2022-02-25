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

package com.hazelcast.cp.internal.datastructures.atomiclong.operation;

import com.hazelcast.cp.IAtomicLong;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.datastructures.atomiclong.AtomicLongDataSerializerHook;
import com.hazelcast.cp.internal.datastructures.atomiclong.AtomicLong;

import java.io.IOException;

/**
 * Operation for {@link IAtomicLong#getAndSet(long)}
 */
public class GetAndSetOp extends AbstractAtomicLongOp {

    private long value;

    public GetAndSetOp() {
    }

    public GetAndSetOp(String name, long value) {
        super(name);
        this.value = value;
    }

    @Override
    public Object run(CPGroupId groupId, long commitIndex) {
        AtomicLong atomic = getAtomicLong(groupId);
        return atomic.getAndSet(value);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeLong(value);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        value = in.readLong();
    }

    @Override
    public int getClassId() {
        return AtomicLongDataSerializerHook.GET_AND_SET_OP;
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);
        sb.append(", value=").append(value);
    }
}
