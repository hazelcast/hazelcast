/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal.datastructures.cpmap.operation;

import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.IAtomicLong;
import com.hazelcast.cp.internal.datastructures.cpmap.CPMapDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.Map;

/**
 * Operation for {@link IAtomicLong#getAndAdd(long)} (long, long)}
 * and {@link IAtomicLong#get()}
 */
public class GetOp extends AbstractCPMapOp {

    private  Object key;

    public GetOp() {
    }

    public GetOp(String name, Object key) {
        super(name);
        this.key = key;
    }

    @Override
    public Object run(CPGroupId groupId, long commitIndex) {
        return getMap(groupId).get(key);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeObject(key);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        this.key = in.readObject();
    }

    @Override
    public int getClassId() {
        return CPMapDataSerializerHook.GET_OP;
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);
        sb.append(", key=").append(key);
    }
}
