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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.impl.FactoryImpl.ProxyKey;
import com.hazelcast.impl.base.RuntimeInterruptedException;
import com.hazelcast.impl.monitor.LocalLockStatsImpl;
import com.hazelcast.impl.monitor.LockOperationsCounter;
import com.hazelcast.monitor.LocalLockStats;
import com.hazelcast.nio.DataSerializable;
import com.hazelcast.nio.SerializationHelper;

@SuppressWarnings("LockAcquiredButNotSafelyReleased")
public class LockProxyImpl extends SerializationHelper implements HazelcastInstanceAwareInstance, LockProxy, DataSerializable {

    private Object key = null;
    private transient LockProxy base = null;
    private transient FactoryImpl factory = null;

    public LockProxyImpl() {
    }

    LockProxyImpl(HazelcastInstance hazelcastInstance, Object key) {
        super();
        this.key = key;
        setHazelcastInstance(hazelcastInstance);
        base = new LockProxyBase();
    }

    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.factory = (FactoryImpl) hazelcastInstance;
    }

    private void ensure() {
        factory.initialChecks();
        if (base == null) {
            base = (LockProxy) factory.getLock(key);
        }
    }

    @Override
    public String toString() {
        return "ILock [" + key + "]";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LockProxyImpl lockProxy = (LockProxyImpl) o;
        return !(key != null ? !key.equals(lockProxy.key) : lockProxy.key != null);
    }

    @Override
    public int hashCode() {
        return key != null ? key.hashCode() : 0;
    }

    public void writeData(DataOutput out) throws IOException {
        writeObject(out, key);
    }

    public void readData(DataInput in) throws IOException {
        key = readObject(in);
        setHazelcastInstance(ThreadContext.get().getCurrentFactory());
    }

    public void lock() {
        ensure();
        base.lock();
    }

    public void lockInterruptibly() throws InterruptedException {
        ensure();
        base.lockInterruptibly();
    }

    public boolean tryLock() {
        ensure();
        return base.tryLock();
    }

    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        ensure();
        return base.tryLock(time, unit);
    }

    public void unlock() {
        ensure();
        base.unlock();
    }

    public Condition newCondition() {
        ensure();
        return base.newCondition();
    }

    public InstanceType getInstanceType() {
        ensure();
        return InstanceType.LOCK;
    }

    public void destroy() {
        ensure();
        base.destroy();
    }

    public Object getLockObject() {
        return key;
    }

    public Object getId() {
        ensure();
        return base.getId();
    }

    public LocalLockStats getLocalLockStats() {
        ensure();
        return base.getLocalLockStats();
    }

    public LockOperationsCounter getLockOperationCounter() {
        ensure();
        return base.getLockOperationCounter();
    }

    private class LockProxyBase implements LockProxy {
        private LockOperationsCounter lockOperationsCounter = new LockOperationsCounter();

        public void lock() {
            factory.locksMapProxy.lock(key);
            lockOperationsCounter.incrementLocks();
        }

        public void lockInterruptibly() throws InterruptedException {
            throw new UnsupportedOperationException("lockInterruptibly is not implemented!");
        }

        public Condition newCondition() {
            return null;
        }

        public boolean tryLock() {
            if (factory.locksMapProxy.tryLock(key)) {
                lockOperationsCounter.incrementLocks();
                return true;
            }
            lockOperationsCounter.incrementFailedLocks();
            return false;
        }

        public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
            try {
                if (factory.locksMapProxy.tryLock(key, time, unit)) {
                    lockOperationsCounter.incrementLocks();
                    return true;
                }
            } catch (RuntimeInterruptedException e) {
                lockOperationsCounter.incrementFailedLocks();
                throw new InterruptedException();
            }
            lockOperationsCounter.incrementFailedLocks();
            return false;
        }

        public void unlock() {
            factory.locksMapProxy.unlock(key);
            lockOperationsCounter.incrementUnlocks();
        }

        public void destroy() {
            factory.destroyInstanceClusterWide("lock", key);
        }

        public InstanceType getInstanceType() {
            return InstanceType.LOCK;
        }

        public Object getLockObject() {
            return key;
        }

        public Object getId() {
            return new ProxyKey("lock", key);
        }

        public LocalLockStats getLocalLockStats() {
            LocalLockStatsImpl localLockStats = new LocalLockStatsImpl();
            localLockStats.setOperationStats(lockOperationsCounter.getPublishedStats());
            return localLockStats;
        }

        public LockOperationsCounter getLockOperationCounter() {
            return lockOperationsCounter;
        }
        
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
		}
    }
}
