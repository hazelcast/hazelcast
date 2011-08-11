/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.client;

import com.hazelcast.core.AtomicNumber;
import com.hazelcast.core.Prefix;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import static com.hazelcast.impl.ClusterOperation.*;

public class AtomicNumberClientProxy implements AtomicNumber {
    private final String name;
    private final ProxyHelper proxyHelper;

    public AtomicNumberClientProxy(HazelcastClient hazelcastClient, String name) {
        this.name = name;
        this.proxyHelper = new ProxyHelper(getName(), hazelcastClient);
    }

    public long addAndGet(long delta) {
        return (Long) proxyHelper.doOp(ATOMIC_NUMBER_ADD_AND_GET, 0L, delta);
    }

    public boolean compareAndSet(long expect, long update) {
        return (Boolean) proxyHelper.doOp(ATOMIC_NUMBER_COMPARE_AND_SET, expect, update);
    }

    public long decrementAndGet() {
        return addAndGet(-1L);
    }

    public long get() {
        return getAndAdd(0L);
    }

    public long getAndAdd(long delta) {
        return (Long) proxyHelper.doOp(ATOMIC_NUMBER_GET_AND_ADD, 0L, delta);
    }

    public long getAndSet(long newValue) {
        return (Long) proxyHelper.doOp(ATOMIC_NUMBER_GET_AND_SET, 0L, newValue);
    }

    public long incrementAndGet() {
        return addAndGet(1L);
    }

    public void set(long newValue) {
        getAndSet(newValue);
    }

    public void destroy() {
        proxyHelper.destroy();
    }

    public Object getId() {
        return name;
    }

    public InstanceType getInstanceType() {
        return InstanceType.ATOMIC_LONG;
    }

    public String getName() {
        return name.substring(Prefix.ATOMIC_NUMBER.length());
    }

    @Deprecated
    public boolean weakCompareAndSet(long expect, long update) {
        throw new NotImplementedException();
    }

    @Deprecated
    public void lazySet(long newValue) {
        throw new NotImplementedException();
    }
}