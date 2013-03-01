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

package com.hazelcast.client;

import com.hazelcast.client.proxy.ProxyHelper;
import com.hazelcast.concurrent.atomiclong.AtomicLongService;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.monitor.LocalAtomicLongStats;
import com.hazelcast.nio.protocol.Command;

public class AtomicLongClientProxy implements IAtomicLong {
    private final String name;
    private final ProxyHelper proxyHelper;

    public AtomicLongClientProxy(HazelcastClient client, String name) {
        this.name = name;
        this.proxyHelper = new ProxyHelper(client);
    }

    public long addAndGet(long delta) {
//        return (Long) proxyHelper.doOp(ATOMIC_NUMBER_ADD_AND_GET, 0L, delta);
        return 0;
    }

    public boolean compareAndSet(long expect, long update) {
//        return (Boolean) proxyHelper.doOp(ATOMIC_NUMBER_COMPARE_AND_SET, expect, update);
        return false;
    }


    public long decrementAndGet() {
        return addAndGet(-1L);
    }

    public long get() {
        return getAndAdd(0L);
    }

    public long getAndAdd(long delta) {
//        return (Long) proxyHelper.doOp(ATOMIC_NUMBER_GET_AND_ADD, 0L, delta);
        return 0;
    }

    public long getAndSet(long newValue) {
//        return (Long) proxyHelper.doOp(ATOMIC_NUMBER_GET_AND_SET, 0L, newValue);
        return 0;
    }

    public long incrementAndGet() {
        return addAndGet(1L);
    }

    public long getAndIncrement() {
        return getAndAdd(1L);
    }

    public void set(long newValue) {
        getAndSet(newValue);
    }

    public void destroy() {
        proxyHelper.doCommand(Command.DESTROY, new String[]{AtomicLongService.SERVICE_NAME, getName()});
    }

    public Object getId() {
        return name;
    }

    public String getName() {
        return name;
    }

    public LocalAtomicLongStats getLocalAtomicLongStats() {
        throw new UnsupportedOperationException();
    }
}