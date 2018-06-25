/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.raft.service.countdownlatch.operation;

import com.hazelcast.core.ICountDownLatch;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.IndeterminateOperationStateAware;
import com.hazelcast.raft.service.countdownlatch.RaftCountDownLatchDataSerializerHook;
import com.hazelcast.raft.service.countdownlatch.RaftCountDownLatchService;

import java.io.IOException;
import java.util.UUID;

/**
 * Operation for {@link ICountDownLatch#countDown()}
 */
public class CountDownOp extends AbstractCountDownLatchOp implements IndeterminateOperationStateAware {

    private int expectedRound;
    private UUID invocationUid;

    public CountDownOp() {
    }

    public CountDownOp(String name, int expectedRound, UUID invocationUid) {
        super(name);
        this.expectedRound = expectedRound;
        this.invocationUid = invocationUid;
    }

    @Override
    public Object run(RaftGroupId groupId, long commitIndex) {
        RaftCountDownLatchService service = getService();
        return service.countDown(groupId, name, expectedRound, invocationUid);
    }

    @Override
    public boolean isRetryableOnIndeterminateOperationState() {
        return true;
    }

    @Override
    public int getId() {
        return RaftCountDownLatchDataSerializerHook.COUNT_DOWN_OP;
    }

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {
        super.writeData(out);
        out.writeInt(expectedRound);
        out.writeLong(invocationUid.getLeastSignificantBits());
        out.writeLong(invocationUid.getMostSignificantBits());
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        super.readData(in);
        expectedRound = in.readInt();
        long least = in.readLong();
        long most = in.readLong();
        invocationUid = new UUID(most, least);
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);
        sb.append(", expectedRound=").append(expectedRound)
          .append(", invocationUid=").append(invocationUid);
    }
}
