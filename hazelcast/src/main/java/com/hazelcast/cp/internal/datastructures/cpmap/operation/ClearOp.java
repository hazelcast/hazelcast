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
import com.hazelcast.cp.internal.datastructures.cpmap.CPMapDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

public class ClearOp extends AbstractCPMapOp {

    public ClearOp() {
    }

    public ClearOp(String name) {
        super(name);
    }

    @Override
    public Object run(CPGroupId groupId, long commitIndex) {
        getMap(groupId).clear();
        return null;
    }

    @Override
    public int getClassId() {
        return CPMapDataSerializerHook.CLEAR_OP;
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);
    }
}
