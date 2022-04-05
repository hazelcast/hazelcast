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

package com.hazelcast.cp.internal.datastructures.countdownlatch.operation;

import com.hazelcast.cp.ICountDownLatch;
import com.hazelcast.cp.internal.datastructures.countdownlatch.CountDownLatchDataSerializerHook;
import com.hazelcast.cp.internal.datastructures.countdownlatch.CountDownLatchService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.cp.CPGroupId;

import java.io.IOException;

/**
 * Operation for {@link ICountDownLatch#trySetCount(int)}
 */
public class TrySetCountOp extends AbstractCountDownLatchOp {

    private int count;

    public TrySetCountOp() {
    }

    public TrySetCountOp(String name, int count) {
        super(name);
        this.count = count;
    }

    @Override
    public Object run(CPGroupId groupId, long commitIndex) {
        CountDownLatchService service = getService();
        return service.trySetCount(groupId, name, count);
    }

    @Override
    public int getClassId() {
        return CountDownLatchDataSerializerHook.TRY_SET_COUNT_OP;
    }

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {
        super.writeData(out);
        out.writeInt(count);
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        super.readData(in);
        count = in.readInt();
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);
        sb.append(", count=").append(count);
    }
}
