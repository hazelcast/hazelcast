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

package com.hazelcast.client.proxy;

import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.concurrent.semaphore.client.*;
import com.hazelcast.core.ISemaphore;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.util.ExceptionUtil;

import java.util.concurrent.TimeUnit;

/**
 * @author ali 5/23/13
 */
public class ClientSemaphoreProxy extends ClientProxy implements ISemaphore {

    private final String name;
    private volatile Data key;

    public ClientSemaphoreProxy(String serviceName, String objectId) {
        super(serviceName, objectId);
        this.name = objectId;
    }

    public boolean init(int permits) {
        checkNegative(permits);
        InitRequest request = new InitRequest(name, permits);
        Boolean result = invoke(request);
        return result;
    }

    public void acquire() throws InterruptedException {
        acquire(1);
    }

    public void acquire(int permits) throws InterruptedException {
        checkNegative(permits);
        AcquireRequest request = new AcquireRequest(name, permits, -1);
        invoke(request);
    }

    public int availablePermits() {
        AvailableRequest request = new AvailableRequest(name);
        Integer result = invoke(request);
        return result;
    }

    public int drainPermits() {
        DrainRequest request = new DrainRequest(name);
        Integer result = invoke(request);
        return result;
    }

    public void reducePermits(int reduction) {
        checkNegative(reduction);
        ReduceRequest request = new ReduceRequest(name, reduction);
        invoke(request);
    }

    public void release() {
        release(1);
    }

    public void release(int permits) {
        checkNegative(permits);
        ReleaseRequest request = new ReleaseRequest(name, permits);
        invoke(request);
    }

    public boolean tryAcquire() {
        return tryAcquire(1);
    }

    public boolean tryAcquire(int permits) {
        checkNegative(permits);
        try {
            return tryAcquire(permits, 0, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            return false;
        }
    }

    public boolean tryAcquire(long timeout, TimeUnit unit) throws InterruptedException {
        return tryAcquire(1, timeout, unit);
    }

    public boolean tryAcquire(int permits, long timeout, TimeUnit unit) throws InterruptedException {
        checkNegative(permits);
        AcquireRequest request = new AcquireRequest(name, permits, unit.toMillis(timeout));
        Boolean result = invoke(request);
        return result;
    }

    protected void onDestroy() {
        SemaphoreDestroyRequest request = new SemaphoreDestroyRequest(name);
        invoke(request);
    }

    public String getName() {
        return name;
    }

    private <T> T invoke(Object req){
        try {
            return getContext().getInvocationService().invokeOnKeyOwner(req, getKey());
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    public Data getKey() {
        if (key == null){
            key = getContext().getSerializationService().toData(name);
        }
        return key;
    }

    private void checkNegative(int permits) {
        if (permits < 0) {
            throw new IllegalArgumentException("Permits cannot be negative!");
        }
    }
}
