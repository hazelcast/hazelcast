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

package com.hazelcast.cp.internal.raft.impl.dto;

import com.hazelcast.cp.internal.raft.impl.RaftEndpoint;
import com.hazelcast.cp.internal.raft.impl.RaftDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.EOFException;
import java.io.IOException;

/**
 * Struct for failure response to AppendEntries RPC.
 * <p>
 * See <i>5.3 Log replication</i> section of
 * <i>In Search of an Understandable Consensus Algorithm</i>
 * paper by <i>Diego Ongaro</i> and <i>John Ousterhout</i>.
 *
 * @see AppendRequest
 * @see AppendSuccessResponse
 */
public class AppendFailureResponse implements IdentifiedDataSerializable {

    /**
     * Value provided when followerNextIndex in failure response should be ignored by receiving leaders
     */
    public static final int SKIPPED_FOLLOWER_NEXT_INDEX = -1;

    private RaftEndpoint follower;
    private int term;
    private long expectedNextIndex;
    private long flowControlSequenceNumber;
    private long followerNextIndex;

    public AppendFailureResponse() {
    }

    public AppendFailureResponse(RaftEndpoint follower, int term, long expectedNextIndex,
                                 long flowControlSequenceNumber, long followerNextIndex) {
        this.follower = follower;
        this.term = term;
        this.expectedNextIndex = expectedNextIndex;
        this.flowControlSequenceNumber = flowControlSequenceNumber;
        this.followerNextIndex = followerNextIndex;
    }

    public RaftEndpoint follower() {
        return follower;
    }

    public int term() {
        return term;
    }

    public long expectedNextIndex() {
        return expectedNextIndex;
    }

    public long flowControlSequenceNumber() {
        return flowControlSequenceNumber;
    }

    /**
     * Provides the expected next log index from this follower, which will be used
     * by the receiving leader to speed up log reconciliation by jumping straight
     * to this index if it does not conflict with its match index.
     *
     * @since 5.6
     * @return the follower's expected next log index
     */
    public long followerNextIndex() {
        return followerNextIndex;
    }

    @Override
    public int getFactoryId() {
        return RaftDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return RaftDataSerializerHook.APPEND_FAILURE_RESPONSE;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(term);
        out.writeObject(follower);
        out.writeLong(expectedNextIndex);
        out.writeLong(flowControlSequenceNumber);
        out.writeLong(followerNextIndex);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        term = in.readInt();
        follower = in.readObject();
        expectedNextIndex = in.readLong();
        try {
            flowControlSequenceNumber = in.readLong();
            // TODO RU_COMPAT_5_3 added for Version 5.3 compatibility. Should be removed at Version 6.0
        } catch (EOFException e) {
            flowControlSequenceNumber = -1;
        }
        try {
            followerNextIndex = in.readLong();
            // TODO RU_COMPAT_5_5 added for Version 5.5 compatibility. Should be removed at Version 6.0
        } catch (EOFException e) {
            followerNextIndex = -1;
        }
    }

    @Override
    public String toString() {
        return "AppendFailureResponse{" + "follower=" + follower + ", term=" + term + ", expectedNextIndex="
                + expectedNextIndex + ", flowControlSequenceNumber=" + flowControlSequenceNumber + ", "
                + "followerNextIndex=" + followerNextIndex + '}';
    }

}
