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

package com.hazelcast.raft.service.atomiclong.operation;

import com.hazelcast.core.IAtomicLong;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.IndeterminateOperationStateAware;
import com.hazelcast.raft.service.atomiclong.RaftAtomicLongDataSerializerHook;
import com.hazelcast.raft.service.atomiclong.RaftAtomicLong;

import java.io.IOException;

/**
 * Operation for {@link IAtomicLong#addAndGet(long)}
 */
public class AddAndGetOp extends AbstractAtomicLongOp implements IndeterminateOperationStateAware {

    private long delta;

    public AddAndGetOp() {
    }

    public AddAndGetOp(String name, long delta) {
        super(name);
        this.delta = delta;
    }

    @Override
    public Object run(RaftGroupId groupId, long commitIndex) {
        RaftAtomicLong atomic = getAtomicLong(groupId);
        return atomic.addAndGet(delta);
    }

    @Override
    public boolean isRetryableOnIndeterminateOperationState() {
        return delta == 0;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeLong(delta);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        delta = in.readLong();
    }

    @Override
    public int getId() {
        return RaftAtomicLongDataSerializerHook.ADD_AND_GET_OP;
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);
        sb.append(", delta=").append(delta);
    }
}
