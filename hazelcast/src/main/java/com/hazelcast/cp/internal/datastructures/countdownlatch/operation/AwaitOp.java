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

package com.hazelcast.cp.internal.datastructures.countdownlatch.operation;

import com.hazelcast.core.ICountDownLatch;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.IndeterminateOperationStateAware;
import com.hazelcast.cp.internal.datastructures.countdownlatch.RaftCountDownLatchDataSerializerHook;
import com.hazelcast.cp.internal.datastructures.countdownlatch.RaftCountDownLatchService;
import com.hazelcast.cp.internal.raft.impl.util.PostponedResponse;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.cp.internal.util.UUIDSerializationUtil.readUUID;
import static com.hazelcast.cp.internal.util.UUIDSerializationUtil.writeUUID;

/**
 * Operation for {@link ICountDownLatch#await(long, TimeUnit)}
 */
public class AwaitOp extends AbstractCountDownLatchOp implements IndeterminateOperationStateAware {

    private UUID invocationUid;
    private long timeoutMillis;

    public AwaitOp() {
    }

    public AwaitOp(String name, UUID invocationUid, long timeoutMillis) {
        super(name);
        this.invocationUid = invocationUid;
        this.timeoutMillis = timeoutMillis;
    }

    @Override
    public Object run(CPGroupId groupId, long commitIndex) {
        RaftCountDownLatchService service = getService();
        if (service.await(groupId, name, commitIndex, invocationUid, timeoutMillis)) {
            return true;
        }

        return timeoutMillis > 0 ? PostponedResponse.INSTANCE : false;
    }

    @Override
    public boolean isRetryableOnIndeterminateOperationState() {
        return true;
    }

    @Override
    public int getId() {
        return RaftCountDownLatchDataSerializerHook.AWAIT_OP;
    }

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {
        super.writeData(out);
        writeUUID(out, invocationUid);
        out.writeLong(timeoutMillis);
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        super.readData(in);
        invocationUid = readUUID(in);
        timeoutMillis = in.readLong();
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);
        sb.append(", invocationUid=").append(invocationUid).append(", timeoutMillis=").append(timeoutMillis);
    }
}
