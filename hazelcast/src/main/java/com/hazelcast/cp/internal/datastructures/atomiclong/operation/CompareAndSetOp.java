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
 * Operation for {@link IAtomicLong#compareAndSet(long, long)}
 */
public class CompareAndSetOp extends AbstractAtomicLongOp {

    private long currentValue;
    private long newValue;

    public CompareAndSetOp() {
    }

    public CompareAndSetOp(String name, long currentValue, long newValue) {
        super(name);
        this.currentValue = currentValue;
        this.newValue = newValue;
    }

    @Override
    public Object run(CPGroupId groupId, long commitIndex) {
        AtomicLong atomic = getAtomicLong(groupId);
        return atomic.compareAndSet(currentValue, newValue);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeLong(currentValue);
        out.writeLong(newValue);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        currentValue = in.readLong();
        newValue = in.readLong();
    }

    @Override
    public int getClassId() {
        return AtomicLongDataSerializerHook.COMPARE_AND_SET_OP;
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);
        sb.append(", currentValue=").append(currentValue);
        sb.append(", newValue=").append(newValue);
    }
}
