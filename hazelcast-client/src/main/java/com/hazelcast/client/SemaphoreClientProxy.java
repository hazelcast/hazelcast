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

import com.hazelcast.core.ISemaphore;
import com.hazelcast.core.Instance;
import com.hazelcast.core.InstanceDestroyedException;
import com.hazelcast.core.Prefix;
import com.hazelcast.monitor.LocalSemaphoreStats;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.impl.ClusterOperation.*;

public class SemaphoreClientProxy implements ISemaphore {

    private final String name;
    private final ProxyHelper proxyHelper;

    public SemaphoreClientProxy(HazelcastClient hazelcastClient, String name) {
        this.name = name;
        proxyHelper = new ProxyHelper(getName(), hazelcastClient);
    }

    public void acquire() throws InstanceDestroyedException, InterruptedException {
        acquire(1);
    }

    public void acquire(int permits) throws InstanceDestroyedException, InterruptedException {
        if (Thread.currentThread().isInterrupted())
            throw new InterruptedException();
        proxyHelper.doOp(SEMAPHORE_TRY_ACQUIRE, false, permits);
    }

    public Future acquireAsync() {
        return acquireAsync(1);
    }

    public Future acquireAsync(int permits) {
        return doAcquireAsync(permits, false);
    }

    public void acquireAttach() throws InstanceDestroyedException, InterruptedException {
        acquireAttach(1);
    }

    public void acquireAttach(int permits) throws InstanceDestroyedException, InterruptedException {
        proxyHelper.doOp(SEMAPHORE_TRY_ACQUIRE, true, permits);
    }

    public Future acquireAttachAsync() {
        return acquireAttachAsync(1);
    }

    public Future acquireAttachAsync(int permits) {
        return doAcquireAsync(permits, true);
    }

    public void attach() {
        attach(1);
    }

    public void attach(int permits) {
        proxyHelper.doOp(SEMAPHORE_ATTACH_DETACH_PERMITS, true, permits);
    }

    public int attachedPermits() {
        return (Integer) proxyHelper.doOp(SEMAPHORE_GET_ATTACHED_PERMITS, false, 0);
    }

    public int availablePermits() {
        return (Integer) proxyHelper.doOp(SEMAPHORE_GET_AVAILABLE_PERMITS, false, 0);
    }

    public void detach() {
        detach(1);
    }

    public void detach(int permits) {
        proxyHelper.doOp(SEMAPHORE_ATTACH_DETACH_PERMITS, false, permits);
    }

    public int drainPermits() {
        return (Integer) proxyHelper.doOp(SEMAPHORE_DRAIN_PERMITS, false, 0);
    }

    public void reducePermits(int permits) {
        proxyHelper.doOp(SEMAPHORE_REDUCE_PERMITS, false, permits);
    }

    public void release() {
        release(1);
    }

    public void release(int permits) {
        proxyHelper.doOp(SEMAPHORE_RELEASE, false, permits);
    }

    public void releaseDetach() {
        releaseDetach(1);
    }

    public void releaseDetach(int permits) {
        proxyHelper.doOp(SEMAPHORE_RELEASE, true, permits);
    }

    public boolean tryAcquire() {
        return tryAcquire(1);
    }

    public boolean tryAcquire(int permits) {
        try {
            return tryAcquire(permits, 0, TimeUnit.MILLISECONDS);
        } catch (Throwable e) {
            return false;
        }
    }

    public boolean tryAcquire(long timeout, TimeUnit unit) throws InstanceDestroyedException, InterruptedException {
        return tryAcquire(1, timeout, unit);
    }

    public boolean tryAcquire(int permits, long timeout, TimeUnit unit) throws InstanceDestroyedException, InterruptedException {
        return (Boolean) proxyHelper.doOpTimeout(SEMAPHORE_TRY_ACQUIRE, false, permits, timeout, unit);
    }

    public boolean tryAcquireAttach() {
        return tryAcquireAttach(1);
    }

    public boolean tryAcquireAttach(int permits) {
        try {
            return tryAcquireAttach(permits, 0, TimeUnit.MILLISECONDS);
        } catch (Throwable e) {
            return false;
        }
    }

    public boolean tryAcquireAttach(long timeout, TimeUnit unit) throws InstanceDestroyedException, InterruptedException {
        return tryAcquireAttach(1, timeout, unit);
    }

    public boolean tryAcquireAttach(int permits, long timeout, TimeUnit unit) throws InstanceDestroyedException, InterruptedException {
        return (Boolean) proxyHelper.doOpTimeout(SEMAPHORE_TRY_ACQUIRE, true, permits, timeout, unit);
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

    public String getName() {
        return name.substring(Prefix.SEMAPHORE.length());
    }

    private Future doAcquireAsync(final int permits, final boolean attach) {
        Packet request = proxyHelper.prepareRequest(SEMAPHORE_TRY_ACQUIRE, attach, permits, Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        Call remoteCall = proxyHelper.createCall(request);
        proxyHelper.sendCall(remoteCall);
        return new AsyncClientCall(remoteCall) {
            public boolean cancel(boolean mayInterruptIfRunning) {
                return !isDone() && (cancelled = (Boolean) proxyHelper.doOp(SEMAPHORE_CANCEL_ACQUIRE, false, 0));
            }
        };
    }

    public LocalSemaphoreStats getLocalSemaphoreStats() {
        throw new UnsupportedOperationException();
    }
}
