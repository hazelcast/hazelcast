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

package com.hazelcast.cp.internal.datastructures.atomiclong;

import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.datastructures.spi.atomic.RaftAtomicValue;

/**
 * State-machine implementation of the Raft-based atomic long
 */
public class AtomicLong extends RaftAtomicValue<Long> {

    private volatile long value;

    AtomicLong(CPGroupId groupId, String name, long value) {
        super(groupId, name);
        this.value = value;
    }

    @SuppressWarnings("NonAtomicOperationOnVolatileField")
    public long addAndGet(long delta) {
        value += delta;
        return value;
    }

    @SuppressWarnings("NonAtomicOperationOnVolatileField")
    public long getAndAdd(long delta) {
        long v = value;
        value += delta;
        return v;
    }

    public long getAndSet(long value) {
        long v = this.value;
        this.value = value;
        return v;
    }

    public boolean compareAndSet(long currentValue, long newValue) {
        if (value == currentValue) {
            value = newValue;
            return true;
        }
        return false;
    }

    public long value() {
        return value;
    }

    @Override
    public Long get() {
        return value;
    }

    @Override
    public String toString() {
        return "AtomicLong{" + "groupId=" + groupId() + ", name='" + name() + '\'' + ", value=" + value + '}';
    }
}
