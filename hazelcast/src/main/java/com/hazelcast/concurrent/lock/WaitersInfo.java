/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.concurrent.lock;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static com.hazelcast.concurrent.lock.LockDataSerializerHook.F_ID;
import static com.hazelcast.concurrent.lock.LockDataSerializerHook.WAITERS_INFO;

final class WaitersInfo implements IdentifiedDataSerializable {

    private String conditionId;
    private Set<ConditionWaiter> waiters = new HashSet<ConditionWaiter>(2);

    public WaitersInfo() {
    }

    public WaitersInfo(String conditionId) {
        this.conditionId = conditionId;
    }

    public void addWaiter(String caller, long threadId) {
        ConditionWaiter waiter = new ConditionWaiter(caller, threadId);
        waiters.add(waiter);
    }

    public void removeWaiter(String caller, long threadId) {
        ConditionWaiter waiter = new ConditionWaiter(caller, threadId);
        waiters.remove(waiter);
    }

    public String getConditionId() {
        return conditionId;
    }

    public boolean hasWaiter() {
        return !waiters.isEmpty();
    }

    public Set<ConditionWaiter> getWaiters() {
        return waiters;
    }

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public int getId() {
        return WAITERS_INFO;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(conditionId);
        int len = waiters.size();
        out.writeInt(len);
        if (len > 0) {
            for (ConditionWaiter w : waiters) {
                out.writeUTF(w.caller);
                out.writeLong(w.threadId);
            }
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        conditionId = in.readUTF();
        int len = in.readInt();
        if (len > 0) {
            for (int i = 0; i < len; i++) {
                ConditionWaiter waiter = new ConditionWaiter(in.readUTF(), in.readLong());
                waiters.add(waiter);
            }
        }
    }

    public static class ConditionWaiter {
        private final String caller;
        private final long threadId;

        ConditionWaiter(String caller, long threadId) {
            this.caller = caller;
            this.threadId = threadId;
        }

        public long getThreadId() {
            return threadId;
        }

        public String getCaller() {
            return caller;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            ConditionWaiter that = (ConditionWaiter) o;

            if (threadId != that.threadId) {
                return false;
            }
            if (caller != null ? !caller.equals(that.caller) : that.caller != null) {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode() {
            int result = caller != null ? caller.hashCode() : 0;
            result = 31 * result + (int) (threadId ^ (threadId >>> 32));
            return result;
        }
    }
}
