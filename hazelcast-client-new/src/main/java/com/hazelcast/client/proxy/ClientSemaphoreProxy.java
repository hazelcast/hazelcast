/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.impl.client.ClientRequest;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.parameters.*;
import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.core.ISemaphore;
import com.hazelcast.nio.serialization.Data;

import java.util.concurrent.TimeUnit;

public class ClientSemaphoreProxy extends ClientProxy implements ISemaphore {

    private final String name;
    private volatile Data key;

    public ClientSemaphoreProxy(String serviceName, String objectId) {
        super(serviceName, objectId);
        this.name = objectId;
    }

    public boolean init(int permits) {
        checkNegative(permits);
        ClientMessage request = SemaphoreInitParameters.encode(name, permits);
        BooleanResultParameters resultParameters = BooleanResultParameters.decode((ClientMessage)invoke(request));

        return resultParameters.result;
    }

    public void acquire() throws InterruptedException {
        ClientMessage request = SemaphoreInitParameters.encode(name, 1);
        invoke(request);
    }

    public void acquire(int permits) throws InterruptedException {
        checkNegative(permits);
        ClientMessage request = SemaphoreInitParameters.encode(name, 1);
        invoke(request);
    }

    public int availablePermits() {
        ClientMessage request = SemaphoreAvailablePermitsParameters.encode(name);
        IntResultParameters resultParameters = IntResultParameters.decode((ClientMessage)invoke(request));
        return resultParameters.result;
    }

    public int drainPermits() {
        ClientMessage request = SemaphoreDrainPermitsParameters.encode(name);
        IntResultParameters resultParameters = IntResultParameters.decode((ClientMessage) invoke(request));
        return resultParameters.result;
    }

    public void reducePermits(int reduction) {
        checkNegative(reduction);
        ClientMessage request = SemaphoreReducePermitsParameters.encode(name, reduction);
        invoke(request);
    }

    public void release() {
        ClientMessage request = SemaphoreReleaseParameters.encode(name, 1);
        invoke(request);
    }

    public void release(int permits) {
        checkNegative(permits);
        ClientMessage request = SemaphoreReleaseParameters.encode(name, permits);
        invoke(request);
    }

    public boolean tryAcquire() {
        ClientMessage request = SemaphoreTryAcquireParameters.encode(name, 1, 0);
        BooleanResultParameters resultParameters = BooleanResultParameters.decode((ClientMessage) invoke(request));
        return resultParameters.result;
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
        if (timeout == 0) {
            return tryAcquire();
        }

        ClientMessage request = SemaphoreTryAcquireParameters.encode(name, 1, unit.toMillis(timeout));
        BooleanResultParameters resultParameters = BooleanResultParameters.decode((ClientMessage) invoke(request));
        return resultParameters.result;
    }

    public boolean tryAcquire(int permits, long timeout, TimeUnit unit) throws InterruptedException {
        checkNegative(permits);
        ClientMessage request = SemaphoreTryAcquireParameters.encode(name, permits, unit.toMillis(timeout));
        BooleanResultParameters resultParameters = BooleanResultParameters.decode((ClientMessage) invoke(request));
        return resultParameters.result;
    }

    protected <T> T invoke(ClientRequest req) {
        return super.invoke(req, getKey());
    }

    public Data getKey() {
        if (key == null) {
            key = getContext().getSerializationService().toData(name);
        }
        return key;
    }

    private void checkNegative(int permits) {
        if (permits < 0) {
            throw new IllegalArgumentException("Permits cannot be negative!");
        }
    }

    @Override
    public String toString() {
        return "ISemaphore{" + "name='" + getName() + '\'' + '}';
    }
}
