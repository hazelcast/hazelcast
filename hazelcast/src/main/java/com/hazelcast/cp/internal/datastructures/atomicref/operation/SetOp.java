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

package com.hazelcast.cp.internal.datastructures.atomicref.operation;

import com.hazelcast.cp.IAtomicReference;
import com.hazelcast.cp.internal.datastructures.atomicref.AtomicRef;
import com.hazelcast.cp.internal.datastructures.atomicref.AtomicRefDataSerializerHook;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.cp.CPGroupId;

import java.io.IOException;

/**
 * Operation for {@link IAtomicReference#set(Object)}
 */
public class SetOp extends AbstractAtomicRefOp implements IdentifiedDataSerializable {

    private Data newValue;
    private boolean returnOldValue;

    public SetOp() {
    }

    public SetOp(String name, Data newValue, boolean returnOldValue) {
        super(name);
        this.newValue = newValue;
        this.returnOldValue = returnOldValue;
    }

    @Override
    public Object run(CPGroupId groupId, long commitIndex) {
        AtomicRef ref = getAtomicRef(groupId);
        Data oldValue = ref.get();
        ref.set(newValue);
        return returnOldValue ? oldValue : null;
    }

    @Override
    public int getClassId() {
        return AtomicRefDataSerializerHook.SET_OP;
    }

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {
        super.writeData(out);
        IOUtil.writeData(out, newValue);
        out.writeBoolean(returnOldValue);
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        super.readData(in);
        newValue = IOUtil.readData(in);
        returnOldValue = in.readBoolean();
    }
}
