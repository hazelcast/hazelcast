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

import com.hazelcast.core.*;
import com.hazelcast.monitor.LocalCountDownLatchStats;

import java.util.concurrent.TimeUnit;

import static com.hazelcast.impl.ClusterOperation.*;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class CountDownLatchClientProxy implements ICountDownLatch {
    private final String name;
    private final ProxyHelper proxyHelper;

    public CountDownLatchClientProxy(HazelcastClient hazelcastClient, String name) {
        this.name = name;
        this.proxyHelper = new ProxyHelper(name, hazelcastClient);
    }

    public void await() throws InstanceDestroyedException, MemberLeftException, InterruptedException {
        await(Long.MAX_VALUE, MILLISECONDS);
    }

    public boolean await(long timeout, TimeUnit unit) throws InstanceDestroyedException, MemberLeftException, InterruptedException {
        try {
            return (Boolean) proxyHelper.doOp(COUNT_DOWN_LATCH_AWAIT, null, null, timeout, unit);
        } catch (RuntimeException re) {
            Throwable e = re.getCause();
            if (e instanceof InstanceDestroyedException) {
                throw (InstanceDestroyedException) e;
            } else if (e instanceof MemberLeftException) {
                throw (MemberLeftException) e;
            } else if (e instanceof InterruptedException) {
                throw (InterruptedException) e;
            } else if (re instanceof NoMemberAvailableException) {
                throw new IllegalStateException();
            }
            throw re;
        }
    }

    public void countDown() {
        proxyHelper.doOp(COUNT_DOWN_LATCH_COUNT_DOWN, null, null);
    }

    public int getCount() {
        return (Integer) proxyHelper.doOp(COUNT_DOWN_LATCH_GET_COUNT, null, null);
    }

    public Member getOwner() {
        return (Member) proxyHelper.doOp(COUNT_DOWN_LATCH_GET_OWNER, null, null);
    }

    public boolean hasCount() {
        return getCount() > 0;
    }

    public boolean setCount(int count) {
        return (Boolean) proxyHelper.doOp(COUNT_DOWN_LATCH_SET_COUNT, null, count);
    }

    public void destroy() {
        proxyHelper.destroy();
    }

    public Object getId() {
        return name;
    }

    public InstanceType getInstanceType() {
        return InstanceType.COUNT_DOWN_LATCH;
    }

    public String getName() {
        return name.substring(Prefix.COUNT_DOWN_LATCH.length());
    }

    public LocalCountDownLatchStats getLocalCountDownLatchStats() {
        throw new UnsupportedOperationException();
    }
}
