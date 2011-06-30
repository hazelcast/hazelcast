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
package com.hazelcast.impl;

import com.hazelcast.core.ISemaphore;
import com.hazelcast.core.Instance;
import com.hazelcast.core.Prefix;
import com.hazelcast.impl.base.FactoryAwareNamedProxy;
import com.hazelcast.nio.Data;

import java.util.concurrent.TimeUnit;

import static com.hazelcast.nio.IOUtil.toData;

public class SemaphoreImpl extends FactoryAwareNamedProxy implements ISemaphore {

    ISemaphore base = null;
    Data nameAsData = null;

    public SemaphoreImpl() {
    }

    public SemaphoreImpl(String name, FactoryImpl factory) {
        setName(name);
        setHazelcastInstance(factory);
        base = new SemaphoreReal();
    }

    Data getNameAsData() {
        if (nameAsData == null) {
            nameAsData = toData(name);
        }
        return nameAsData;
    }

    public Object getId() {
        ensure();
        return base.getId();
    }

    public void destroy() {
        ensure();
        base.destroy();
    }

    public InstanceType getInstanceType() {
        return InstanceType.SEMAPHORE;
    }

    public boolean tryAcquire(int permits, long timeout, TimeUnit unit) {
        ensure();
        return base.tryAcquire(permits, timeout, unit);
    }

    public boolean tryAcquire(int permits) {
        ensure();
        return base.tryAcquire(permits);
    }

    public boolean tryAcquire(long timeout, TimeUnit unit) {
        ensure();
        return base.tryAcquire(timeout, unit);
    }

    public boolean tryAcquire() {
        ensure();
        return base.tryAcquire();
    }

    public void release(int permits) {
        ensure();
        base.release(permits);
    }

    public void release() {
        ensure();
        base.release();
    }

    public int drainPermits() {
        ensure();
        return base.drainPermits();
    }

    public int availablePermits() {
        ensure();
        return base.availablePermits();
    }

    public void acquireUninterruptibly(int permits) {
        ensure();
        base.acquireUninterruptibly(permits);
    }

    public void acquireUninterruptibly() {
        ensure();
        base.acquireUninterruptibly();
    }

    public void acquire(int permits) throws InterruptedException {
        ensure();
        base.acquire(permits);
    }

    public void acquire() throws InterruptedException {
        ensure();
        base.acquire();
    }

    public void reducePermits(int permits) {
        ensure();
        base.reducePermits(permits);
    }

    private void ensure() {
        factory.initialChecks();
        if (base == null) {
            base = (ISemaphore) factory.getOrCreateProxyByName(name);
        }
    }

    private class SemaphoreReal implements ISemaphore {

        ConcurrentMapManager.MSemaphore newMSemaphore(ClusterOperation op, int value) {
            return factory.node.concurrentMapManager.new MSemaphore(getNameAsData(), op, value);
        }

        public String getName() {
            return name.substring(Prefix.SEMAPHORE.length());
        }

        public void acquire() throws InterruptedException {
            tryAcquire();
        }

        public void acquireUninterruptibly() {
            tryAcquire();
        }

        public boolean tryAcquire(int permits) {
            return tryAcquire(permits, -1, TimeUnit.MILLISECONDS);
        }

        public boolean tryAcquire() {
            return tryAcquire(1);
        }

        public boolean tryAcquire(long timeout, TimeUnit unit) {
            return tryAcquire(1, timeout, unit);
        }

        public boolean tryAcquire(int permits, long timeout, TimeUnit unit) {
            ConcurrentMapManager.MSemaphore s = newMSemaphore(ClusterOperation.SEMAPHORE_ACQUIRE, 1);
            return s.tryAcquire(permits, timeout, unit);
        }

        public void release() {
            release(1);
        }

        public void acquire(int permits) throws InterruptedException {
            tryAcquire(permits);
        }

        public void acquireUninterruptibly(int permits) {
            tryAcquire(permits);
        }

        public void release(int permits) {
            ConcurrentMapManager.MSemaphore s = newMSemaphore(ClusterOperation.SEMAPHORE_RELEASE, 1);
            s.tryRelease(permits, -1, TimeUnit.MILLISECONDS);
        }

        public int availablePermits() {
            ConcurrentMapManager.MSemaphore s = newMSemaphore(ClusterOperation.SEMAPHORE_AVAILABLE_PERIMITS, 0);
            int result = s.availablePermits();
            return (Integer) result;
        }

        public int drainPermits() {
            ConcurrentMapManager.MSemaphore s = newMSemaphore(ClusterOperation.SEMAPHORE_DRAIN_PERIMITS, 0);
            int result = s.drainPermits();
            return (Integer) result;
        }

        public void reducePermits(int permits) {
            ConcurrentMapManager.MSemaphore s = newMSemaphore(ClusterOperation.SEMAPHORE_REDUCE_PERIMITS, permits);
            s.reducePermits(permits);
        }

        public InstanceType getInstanceType() {
            return Instance.InstanceType.SEMAPHORE;
        }

        public void destroy() {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        public Object getId() {
            throw new UnsupportedOperationException("Not supported yet.");
        }
    }
}
