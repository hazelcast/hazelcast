/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.cp.internal.datastructures.cpmap.CPMapDataSerializerHook;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

public class CPMapCompareAndSetOp extends CPMapRaftOp {
    private Data key;
    private Data expectedValue;
    private Data newValue;

    public CPMapCompareAndSetOp() {
    }

    public CPMapCompareAndSetOp(String objectName, Data key, Data expectedValue, Data newValue) {
        super(objectName);
        this.key = key;
        this.expectedValue = expectedValue;
        this.newValue = newValue;
    }

    @Override
    public Object run(CPGroupId groupId, long commitIndex) throws Exception {
        return getMap(groupId, objectName).compareAndSet(key, expectedValue, newValue, getLeaderTimestamp());
    }

    @Override
    public int getClassId() {
        return CPMapDataSerializerHook.CAS_OP;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        IOUtil.writeData(out, key);
        IOUtil.writeData(out, expectedValue);
        IOUtil.writeData(out, newValue);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        key = IOUtil.readData(in);
        expectedValue = IOUtil.readData(in);
        newValue = IOUtil.readData(in);
    }
}
