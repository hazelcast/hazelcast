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
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.IndeterminateOperationStateAware;
import com.hazelcast.cp.internal.datastructures.countdownlatch.CountDownLatchDataSerializerHook;
import com.hazelcast.cp.internal.datastructures.countdownlatch.CountDownLatchService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.UUID;

import static com.hazelcast.internal.util.UUIDSerializationUtil.readUUID;
import static com.hazelcast.internal.util.UUIDSerializationUtil.writeUUID;

/**
 * Operation for {@link ICountDownLatch#countDown()}
 */
public class CountDownOp extends AbstractCountDownLatchOp implements IndeterminateOperationStateAware {

    private UUID invocationUid;
    private int expectedRound;

    public CountDownOp() {
    }

    public CountDownOp(String name, UUID invocationUid, int expectedRound) {
        super(name);
        this.invocationUid = invocationUid;
        this.expectedRound = expectedRound;
    }

    @Override
    public Object run(CPGroupId groupId, long commitIndex) {
        CountDownLatchService service = getService();
        return service.countDown(groupId, name, invocationUid, expectedRound);
    }

    @Override
    public boolean isRetryableOnIndeterminateOperationState() {
        return true;
    }

    @Override
    public int getClassId() {
        return CountDownLatchDataSerializerHook.COUNT_DOWN_OP;
    }

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {
        super.writeData(out);
        writeUUID(out, invocationUid);
        out.writeInt(expectedRound);
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        super.readData(in);
        invocationUid = readUUID(in);
        expectedRound = in.readInt();
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);
        sb.append(", invocationUid=").append(invocationUid)
          .append(", expectedRound=").append(expectedRound);
    }
}
