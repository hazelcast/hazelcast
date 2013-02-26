/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.concurrent.atomiclong.proxy;

import com.hazelcast.concurrent.atomiclong.AtomicLongService;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.monitor.LocalAtomicLongStats;
import com.hazelcast.spi.NodeEngine;

// author: sancar - 21.12.2012
public class AtomicLongProxy extends AtomicLongProxySupport implements IAtomicLong {

    public AtomicLongProxy(String name, NodeEngine nodeEngine, AtomicLongService service) {
        super(name, nodeEngine, service);
    }

    public long addAndGet(long delta) {
        return addAndGetInternal(delta);
    }

    public boolean compareAndSet(long expect, long update) {
        return compareAndSetInternal(expect, update);
    }

    public long decrementAndGet() {
        return addAndGetInternal(-1);
    }

    public long incrementAndGet() {
        return addAndGetInternal(1);
    }

    public long getAndIncrement() {
        return getAndAdd(1);
    }

    public long get() {
        return getAndAdd(0);
    }

    public long getAndAdd(long delta) {
        return getAndAddInternal(delta);
    }

    public long getAndSet(long newValue) {
        return getAndSetInternal(newValue);
    }

    public void set(long newValue) {
        setInternal(newValue);
    }

}
