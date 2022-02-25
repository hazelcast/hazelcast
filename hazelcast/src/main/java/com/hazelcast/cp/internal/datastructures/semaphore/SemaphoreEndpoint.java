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

package com.hazelcast.cp.internal.datastructures.semaphore;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

/**
 * SemaphoreEndpoint represents a thread in a Raft client. It is a combination
 * of a session id and a thread id. A SemaphoreEndpoint is a single-threaded
 * unique entity. A semaphore can be used without a session. Therefore,
 * A SemaphoreEndpoint can be created without a session id. When it sends
 * a request X, it can either retry this request X, or send a new request Y.
 * After it sends request Y, it will not retry request X anymore.
 */
public class SemaphoreEndpoint implements IdentifiedDataSerializable {
    private long sessionId;
    private long threadId;

    public SemaphoreEndpoint() {
    }

    public SemaphoreEndpoint(long sessionId, long threadId) {
        this.sessionId = sessionId;
        this.threadId = threadId;
    }

    public long sessionId() {
        return sessionId;
    }

    public long threadId() {
        return threadId;
    }

    @Override
    public int getFactoryId() {
        return SemaphoreDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SemaphoreDataSerializerHook.SEMAPHORE_ENDPOINT;
    }

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {
        out.writeLong(sessionId);
        out.writeLong(threadId);
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        sessionId = in.readLong();
        threadId = in.readLong();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SemaphoreEndpoint)) {
            return false;
        }

        SemaphoreEndpoint that = (SemaphoreEndpoint) o;

        if (sessionId != that.sessionId) {
            return false;
        }
        return threadId == that.threadId;
    }

    @Override
    public int hashCode() {
        int result = (int) (sessionId ^ (sessionId >>> 32));
        result = 31 * result + (int) (threadId ^ (threadId >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "SemaphoreEndpoint{" + "sessionId=" + sessionId + ", threadId=" + threadId + '}';
    }
}

