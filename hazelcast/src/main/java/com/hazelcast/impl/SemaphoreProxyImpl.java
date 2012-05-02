/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

package com.hazelcast.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.InstanceDestroyedException;
import com.hazelcast.core.Prefix;
import com.hazelcast.core.RuntimeInterruptedException;
import com.hazelcast.impl.base.FactoryAwareNamedProxy;
import com.hazelcast.impl.monitor.LocalSemaphoreStatsImpl;
import com.hazelcast.impl.monitor.SemaphoreOperationsCounter;
import com.hazelcast.monitor.LocalSemaphoreStats;
import com.hazelcast.nio.Data;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.nio.IOUtil.toData;

public class SemaphoreProxyImpl extends FactoryAwareNamedProxy implements SemaphoreProxy {
    private transient SemaphoreProxy base = null;
    Data nameAsData = null;

    public SemaphoreProxyImpl(String name, FactoryImpl factory) {
        setName(name);
        setHazelcastInstance(factory);
        base = new SemaphoreProxyReal();
    }

    private void ensure() {
        factory.initialChecks();
        if (base == null) {
            base = (SemaphoreProxy) factory.getOrCreateProxyByName(name);
        }
    }

    public String getLongName() {
        return name;
    }

    public String getName() {
        return name.substring(Prefix.SEMAPHORE.length());
    }

    Data getNameAsData() {
        if (nameAsData == null) {
            nameAsData = toData(getName());
        }
        return nameAsData;
    }

    public Object getId() {
        ensure();
        return base.getId();
    }

    @Override
    public String toString() {
        return "Semaphore [" + getName() + "]";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        SemaphoreProxyImpl that = (SemaphoreProxyImpl) o;
        return !(name != null ? !name.equals(that.name) : that.name != null);
    }

    @Override
    public int hashCode() {
        return name != null ? name.hashCode() : 0;
    }

    public InstanceType getInstanceType() {
        ensure();
        return base.getInstanceType();
    }

    public LocalSemaphoreStats getLocalSemaphoreStats() {
        ensure();
        return base.getLocalSemaphoreStats();
    }

    public SemaphoreOperationsCounter getOperationsCounter() {
        ensure();
        return base.getOperationsCounter();
    }

    public void acquire() throws InstanceDestroyedException, InterruptedException {
        ensure();
        base.acquire();
    }

    public void acquire(int permits) throws InstanceDestroyedException, InterruptedException {
        check(permits);
        ensure();
        base.acquire(permits);
    }

    public Future acquireAsync() {
        return doAsyncAcquire(1, false);
    }

    public Future acquireAsync(int permits) {
        check(permits);
        return doAsyncAcquire(permits, false);
    }

    public void acquireAttach() throws InstanceDestroyedException, InterruptedException {
        ensure();
        base.acquireAttach();
    }

    public void acquireAttach(int permits) throws InstanceDestroyedException, InterruptedException {
        check(permits);
        ensure();
        base.acquireAttach(permits);
    }

    public Future acquireAttachAsync() {
        return doAsyncAcquire(1, true);
    }

    public Future acquireAttachAsync(int permits) {
        check(permits);
        return doAsyncAcquire(permits, true);
    }

    public void attach() {
        ensure();
        base.attach();
    }

    public void attach(int permits) {
        check(permits);
        ensure();
        base.attach(permits);
    }

    public int attachedPermits() {
        ensure();
        return base.attachedPermits();
    }

    public int availablePermits() {
        ensure();
        return base.availablePermits();
    }

    public void detach() {
        ensure();
        base.detach();
    }

    public void detach(int permits) {
        check(permits);
        ensure();
        base.detach(permits);
    }

    public void destroy() {
        ensure();
        base.destroy();
    }

    public int drainPermits() {
        ensure();
        return base.drainPermits();
    }

    public void reducePermits(int permits) {
        check(permits);
        ensure();
        base.reducePermits(permits);
    }

    public void release() {
        ensure();
        base.release();
    }

    public void release(int permits) {
        check(permits);
        ensure();
        base.release(permits);
    }

    public void releaseDetach() {
        ensure();
        base.releaseDetach();
    }

    public void releaseDetach(int permits) {
        check(permits);
        ensure();
        base.releaseDetach(permits);
    }

    public boolean tryAcquire() {
        ensure();
        return base.tryAcquire();
    }

    public boolean tryAcquire(int permits) {
        check(permits);
        ensure();
        return base.tryAcquire(permits);
    }

    public boolean tryAcquire(long timeout, TimeUnit unit) throws InstanceDestroyedException, InterruptedException {
        ensure();
        return base.tryAcquire(timeout, unit);
    }

    public boolean tryAcquire(int permits, long timeout, TimeUnit timeunit) throws InstanceDestroyedException, InterruptedException {
        check(permits, timeout, timeunit);
        ensure();
        return base.tryAcquire(permits, timeout, timeunit);
    }

    public boolean tryAcquireAttach() {
        ensure();
        return base.tryAcquireAttach();
    }

    public boolean tryAcquireAttach(int permits) {
        check(permits);
        ensure();
        return base.tryAcquireAttach(permits);
    }

    public boolean tryAcquireAttach(long timeout, TimeUnit timeunit) throws InstanceDestroyedException, InterruptedException {
        ensure();
        return base.tryAcquireAttach(timeout, timeunit);
    }

    public boolean tryAcquireAttach(int permits, long timeout, TimeUnit timeunit) throws InstanceDestroyedException, InterruptedException {
        check(permits, timeout, timeunit);
        ensure();
        return base.tryAcquireAttach(permits, timeout, timeunit);
    }

    private void check(int permits) {
        if (permits < 0)
            throw new IllegalArgumentException("Number of permits can not be negative: " + permits);
    }

    private void check(int permits, long timeout, TimeUnit timeunit) {
        check(permits);
        if (timeout < -1)
            throw new IllegalArgumentException("Invalid timeout value: " + timeout);
        if (timeunit == null) {
            throw new NullPointerException("TimeUnit can not be null.");
        }
    }

    private Future doAsyncAcquire(final Integer permits, final Boolean attach) {
        final SemaphoreProxyImpl semaphoreProxy = SemaphoreProxyImpl.this;
        AsyncCall call = new AsyncCall() {
            @Override
            protected void call() {
                try {
                    if (attach)
                        semaphoreProxy.acquireAttach(permits);
                    else
                        semaphoreProxy.acquire(permits);
                    setResult(null);
                } catch (InterruptedException e) {
                    setResult(e);
                } catch (InstanceDestroyedException e) {
                    e.printStackTrace();
                }
            }

            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                ConcurrentMapManager.MSemaphore msemaphore = factory.node.concurrentMapManager.new MSemaphore();
                return msemaphore.cancelAcquire(getNameAsData());
            }
        };
        factory.node.executorManager.executeAsync(call);
        return call;
    }

    private class SemaphoreProxyReal implements SemaphoreProxy {
        SemaphoreOperationsCounter operationsCounter = new SemaphoreOperationsCounter();

        public Object getId() {
            return name;
        }

        public InstanceType getInstanceType() {
            return InstanceType.SEMAPHORE;
        }

        public String getLongName() {
            return name;
        }

        public String getName() {
            return name.substring(Prefix.SEMAPHORE.length());
        }

        public void destroy() {
            newMSemaphore().destroy(getNameAsData());
            factory.destroyInstanceClusterWide(name, null);
        }

        public void acquire() throws InstanceDestroyedException, InterruptedException {
            acquire(1);
        }

        public void acquire(int permits) throws InstanceDestroyedException, InterruptedException {
            if (Thread.interrupted())
                throw new InterruptedException();
            try {
                doTryAcquire(permits, false, -1);
            } catch (RuntimeInterruptedException e) {
                throw new InterruptedException();
            }
        }

        public Future acquireAsync() {
            throw new UnsupportedOperationException();
        }

        public Future acquireAsync(int permits) {
            throw new UnsupportedOperationException();
        }

        public void acquireAttach() throws InstanceDestroyedException, InterruptedException {
            acquireAttach(1);
        }

        public void acquireAttach(int permits) throws InstanceDestroyedException, InterruptedException {
            if (Thread.interrupted())
                throw new InterruptedException();
            try {
                doTryAcquire(permits, true, -1);
            } catch (RuntimeInterruptedException e) {
                throw new InterruptedException();
            }
        }

        public Future acquireAttachAsync() {
            throw new UnsupportedOperationException();
        }

        public Future acquireAttachAsync(int permits) {
            throw new UnsupportedOperationException();
        }

        public void attach() {
            attach(1);
        }

        public void attach(int permits) {
            newMSemaphore().attachDetach(getNameAsData(), permits);
        }

        public int attachedPermits() {
            return newMSemaphore().getAttached(getNameAsData());
        }

        public int availablePermits() {
            return newMSemaphore().getAvailable(getNameAsData());
        }

        public void detach() {
            detach(1);
        }

        public void detach(int permits) {
            newMSemaphore().attachDetach(getNameAsData(), -permits);
        }

        public int drainPermits() {
            return newMSemaphore().drainPermits(getNameAsData());
        }

        public void release() {
            release(1);
        }

        public void release(int permits) {
            newMSemaphore().release(getNameAsData(), permits, false);
        }

        public void releaseDetach() {
            releaseDetach(1);
        }

        public void releaseDetach(int permits) {
            newMSemaphore().release(getNameAsData(), permits, true);
        }

        public boolean tryAcquire() {
            return tryAcquire(1);
        }

        public boolean tryAcquire(int permits) {
            try {
                return doTryAcquire(permits, false, -1);
            } catch (Throwable e) {
                return false;
            }
        }

        public boolean tryAcquire(long timeout, TimeUnit unit) throws InstanceDestroyedException, InterruptedException {
            return tryAcquire(1, timeout, unit);
        }

        public boolean tryAcquire(int permits, long timeout, TimeUnit unit) throws InstanceDestroyedException, InterruptedException {
            if (Thread.interrupted())
                throw new InterruptedException();
            try {
                return doTryAcquire(permits, false, unit.toMillis(timeout));
            } catch (RuntimeInterruptedException e) {
                throw new InterruptedException();
            }
        }

        public boolean tryAcquireAttach() {
            return tryAcquireAttach(1);
        }

        public boolean tryAcquireAttach(int permits) {
            try {
                return doTryAcquire(permits, true, -1);
            } catch (Throwable e) {
                return false;
            }
        }

        public boolean tryAcquireAttach(long timeout, TimeUnit unit) throws InstanceDestroyedException, InterruptedException {
            return tryAcquireAttach(1, timeout, unit);
        }

        public boolean tryAcquireAttach(int permits, long timeout, TimeUnit unit) throws InstanceDestroyedException, InterruptedException {
            if (Thread.interrupted())
                throw new InterruptedException();
            try {
                return doTryAcquire(permits, true, unit.toMillis(timeout));
            } catch (RuntimeInterruptedException e) {
                throw new InterruptedException();
            }
        }

        public void reducePermits(int permits) {
            newMSemaphore().reduce(getNameAsData(), permits);
        }

        public LocalSemaphoreStats getLocalSemaphoreStats() {
            LocalSemaphoreStatsImpl localSemaphoreStats = new LocalSemaphoreStatsImpl();
            localSemaphoreStats.setOperationStats(operationsCounter.getPublishedStats());
            return localSemaphoreStats;
        }

        public SemaphoreOperationsCounter getOperationsCounter() {
            return operationsCounter;
        }

        private ConcurrentMapManager.MSemaphore newMSemaphore() {
            ConcurrentMapManager.MSemaphore msemaphore = factory.node.concurrentMapManager.new MSemaphore();
            msemaphore.setOperationsCounter(operationsCounter);
            return msemaphore;
        }

        private boolean doTryAcquire(int permits, boolean attach, long timeout) throws InstanceDestroyedException {
            return newMSemaphore().tryAcquire(getNameAsData(), permits, attach, timeout);
        }

        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        }
    }
}