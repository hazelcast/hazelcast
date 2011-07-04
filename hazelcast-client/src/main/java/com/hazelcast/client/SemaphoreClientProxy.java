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

import com.hazelcast.core.ISemaphore;
import com.hazelcast.core.Instance;
import com.hazelcast.core.Prefix;
import com.hazelcast.impl.ClusterOperation;

import java.util.concurrent.TimeUnit;

public class SemaphoreClientProxy implements ISemaphore {

    private final String name;
    private final ProxyHelper proxyHelper;
    private final HazelcastClient client;

    public SemaphoreClientProxy(HazelcastClient hazelcastClient, String name) {
        this.name = name;
        this.client = hazelcastClient;
        proxyHelper = new ProxyHelper(name, hazelcastClient);
    }

    public boolean tryAcquire() {
        return tryAcquire(1);
    }

    public boolean tryAcquire(long timeout, TimeUnit unit) {
        return tryAcquire(1, timeout, unit);
    }

    public boolean tryAcquire(int permits) {
        return tryAcquire(permits, -1, TimeUnit.NANOSECONDS);
    }

    public boolean tryAcquire(int permits, long timeout, TimeUnit timeUnit) {
        Boolean result = false;
        long start = System.currentTimeMillis();
        Integer remaining = (Integer) proxyHelper.doOp(ClusterOperation.SEMAPHORE_ACQUIRE, null, permits);
        long end = System.currentTimeMillis();
        //Estimate the invocation time, so that it can be deducted from the timeout.
        long diff = end - start;
        if (remaining > 0) {
            if (timeout > 0 && timeout > diff) {
                result = tryAcquire(remaining, timeout - diff, timeUnit);
            } else if (timeout < 0) {
                result = tryAcquire(remaining, timeout, timeUnit);
            } else {
                result = false;
            }
        } else result = true;
        if (!result) {
            tryRelease(permits - remaining, -1, TimeUnit.MILLISECONDS);
        }
        return result;
    }

    public boolean tryRelease(int permits, long timeout, TimeUnit timeUnit) {
        proxyHelper.doOp(ClusterOperation.SEMAPHORE_RELEASE, null, permits);
        return true;
    }

    public void acquireUninterruptibly() {
        tryAcquire();
    }

    public void acquireUninterruptibly(int permits) {
        tryAcquire(permits);
    }

    public String getName() {
        return name.substring(Prefix.SEMAPHORE.length());
    }

    public void acquire() throws InterruptedException {
        tryAcquire();
    }

    public void acquire(int permits) throws InterruptedException {
        tryAcquire(permits);
    }

    public void release() {
        tryRelease(1, -1, TimeUnit.NANOSECONDS);
    }

    public void release(int permits) {
        tryRelease(permits, -1, TimeUnit.NANOSECONDS);
    }

    public int availablePermits() {
        return (Integer) proxyHelper.doOp(ClusterOperation.SEMAPHORE_AVAILABLE_PERMITS, null, null);
    }

    public int drainPermits() {
        return (Integer) proxyHelper.doOp(ClusterOperation.SEMAPHORE_DRAIN_PERMITS, null, null);
    }

    public void reducePermits(int permits) {
        proxyHelper.doOp(ClusterOperation.SEMAPHORE_REDUCE_PERMITS, null, permits);
    }

    public InstanceType getInstanceType() {
        return Instance.InstanceType.SEMAPHORE;
    }

    public void destroy() {
        proxyHelper.destroy();
    }

    public Object getId() {
        return name;
    }
}
