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
import com.hazelcast.impl.ClusterOperation;

public class AtomicNumberClientProxy implements AtomicNumber {
    private final String name;
    private final ProxyHelper proxyHelper;

    public AtomicNumberClientProxy(HazelcastClient hazelcastClient, String name) {
        this.name = name;
        proxyHelper = new ProxyHelper(name, hazelcastClient);
    }

    public String getName() {
        return name.substring(Prefix.ATOMIC_NUMBER.length());
    }

    public long addAndGet(long delta) {
        return setLongGetLong(ClusterOperation.ATOMIC_NUMBER_ADD_AND_GET, delta);
    }

    private long setLongGetLong(ClusterOperation operation, long value) {
        Packet request = proxyHelper.prepareRequest(operation, null, null);
        request.setLongValue(value);
        Packet response = proxyHelper.callAndGetResult(request);
        return (Long) proxyHelper.getValue(response);
    }

    public boolean compareAndSet(long expect, long update) {
        return false;
    }

    public boolean weakCompareAndSet(long expect, long update) {
        return false;
    }

    public long decrementAndGet() {
        return setLongGetLong(ClusterOperation.ATOMIC_NUMBER_ADD_AND_GET, -1L);
    }

    public long get() {
        return setLongGetLong(ClusterOperation.ATOMIC_NUMBER_GET_AND_SET, 0L);
    }

    public long getAndAdd(long delta) {
        return setLongGetLong(ClusterOperation.ATOMIC_NUMBER_GET_AND_ADD, delta);
    }

    public long getAndSet(long newValue) {
        return setLongGetLong(ClusterOperation.ATOMIC_NUMBER_GET_AND_SET, newValue);
    }

    public long incrementAndGet() {
        return setLongGetLong(ClusterOperation.ATOMIC_NUMBER_ADD_AND_GET, 1L);
    }

    public void lazySet(long newValue) {
        set(newValue);
    }

    public void set(long newValue) {
        getAndSet(newValue);
    }

    public InstanceType getInstanceType() {
        return InstanceType.ATOMIC_NUMBER;
    }

    public void destroy() {
        proxyHelper.destroy();
    }

    public Object getId() {
        return name;
    }
}