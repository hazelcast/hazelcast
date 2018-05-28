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

package com.hazelcast.concurrent.atomiclong;

public class AtomicLongContainer {

    private long value;

    public AtomicLongContainer() {
    }

    public long get() {
        return value;
    }

    public long addAndGet(long delta) {
        value += delta;
        return value;
    }

    public void set(long value) {
        this.value = value;
    }

    public boolean compareAndSet(long expect, long value) {
        if (this.value != expect) {
            return false;
        }
        this.value = value;
        return true;
    }

    public long getAndAdd(long delta) {
        long tempValue = value;
        value += delta;
        return tempValue;
    }

    public long getAndSet(long value) {
        long tempValue = this.value;
        this.value = value;
        return tempValue;
    }
}
