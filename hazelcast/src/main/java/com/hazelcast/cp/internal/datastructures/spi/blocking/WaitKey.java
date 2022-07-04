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

package com.hazelcast.cp.internal.datastructures.spi.blocking;

import com.hazelcast.cp.internal.session.AbstractProxySessionManager;
import com.hazelcast.cluster.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.UUID;

import static com.hazelcast.internal.util.UUIDSerializationUtil.readUUID;
import static com.hazelcast.internal.util.UUIDSerializationUtil.writeUUID;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * This abstraction is used when an operation does not return a response
 * at commit-time. Such an operation will either return a response in future
 * or timeout. Future completions of such operations are represented with
 * implementations of this interface.
 */
public abstract class WaitKey implements IdentifiedDataSerializable {

    protected long commitIndex;
    protected UUID invocationUid;
    protected Address callerAddress;
    protected long callId;

    public WaitKey() {
    }

    public WaitKey(long commitIndex, UUID invocationUid, Address callerAddress, long callId) {
        checkNotNull(invocationUid);
        checkNotNull(callerAddress);
        this.commitIndex = commitIndex;
        this.invocationUid = invocationUid;
        this.callerAddress = callerAddress;
        this.callId = callId;
    }

    /**
     * Returns id of the session to which the corresponding operation is
     * attached.
     * Returns {@link AbstractProxySessionManager#NO_SESSION_ID} if no session
     *         is attached.
     */
    public abstract long sessionId();

    /**
     * Returns commit index of the operation that has not returned a response
     * at the time of its commit.
     */
    public final long commitIndex() {
        return commitIndex;
    }

    /**
     * Returns unique id of the committed operation which is provided by the caller.
     */
    public final UUID invocationUid() {
        return invocationUid;
    }

    /**
     * Returns address of the caller which created this wait key
     */
    public final Address callerAddress() {
        return callerAddress;
    }

    /**
     * Returns the call id which created this wait key
     */
    public final long callId() {
        return callId;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(commitIndex);
        writeUUID(out, invocationUid);
        out.writeObject(callerAddress);
        out.writeLong(callId);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        commitIndex = in.readLong();
        invocationUid = readUUID(in);
        callerAddress = in.readObject();
        callId = in.readLong();
    }

}
