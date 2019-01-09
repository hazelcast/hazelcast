/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.SemaphoreAcquireCodec;
import com.hazelcast.client.impl.protocol.codec.SemaphoreAvailablePermitsCodec;
import com.hazelcast.client.impl.protocol.codec.SemaphoreDrainPermitsCodec;
import com.hazelcast.client.impl.protocol.codec.SemaphoreIncreasePermitsCodec;
import com.hazelcast.client.impl.protocol.codec.SemaphoreInitCodec;
import com.hazelcast.client.impl.protocol.codec.SemaphoreReducePermitsCodec;
import com.hazelcast.client.impl.protocol.codec.SemaphoreReleaseCodec;
import com.hazelcast.client.impl.protocol.codec.SemaphoreTryAcquireCodec;
import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.core.ISemaphore;

import java.util.concurrent.TimeUnit;

import static java.lang.Thread.currentThread;

/**
 * Proxy implementation of {@link ISemaphore}.
 */
public class ClientSemaphoreProxy extends PartitionSpecificClientProxy implements ISemaphore {

    public ClientSemaphoreProxy(String serviceName, String objectId, ClientContext context) {
        super(serviceName, objectId, context);
    }

    @Override
    public boolean init(int permits) {
        checkNegative(permits);
        ClientMessage request = SemaphoreInitCodec.encodeRequest(name, permits);
        ClientMessage response = invokeOnPartition(request);
        SemaphoreInitCodec.ResponseParameters resultParameters = SemaphoreInitCodec.decodeResponse(response);

        return resultParameters.response;
    }

    @Override
    public void acquire() throws InterruptedException {
        ClientMessage request = SemaphoreAcquireCodec.encodeRequest(name, 1);
        invokeOnPartition(request);
    }

    @Override
    public void acquire(int permits) throws InterruptedException {
        checkNegative(permits);
        ClientMessage request = SemaphoreAcquireCodec.encodeRequest(name, permits);
        invokeOnPartition(request);
    }

    @Override
    public int availablePermits() {
        ClientMessage request = SemaphoreAvailablePermitsCodec.encodeRequest(name);
        ClientMessage response = invokeOnPartition(request);
        SemaphoreAvailablePermitsCodec.ResponseParameters resultParameters
                = SemaphoreAvailablePermitsCodec.decodeResponse(response);
        return resultParameters.response;
    }

    @Override
    public int drainPermits() {
        ClientMessage request = SemaphoreDrainPermitsCodec.encodeRequest(name);
        ClientMessage response = invokeOnPartition(request);
        SemaphoreDrainPermitsCodec.ResponseParameters resultParameters
                = SemaphoreDrainPermitsCodec.decodeResponse(response);
        return resultParameters.response;
    }

    @Override
    public void reducePermits(int reduction) {
        checkNegative(reduction);
        ClientMessage request = SemaphoreReducePermitsCodec.encodeRequest(name, reduction);
        invokeOnPartition(request);
    }

    @Override
    public void increasePermits(int increase) {
        checkNegative(increase);
        ClientMessage request = SemaphoreIncreasePermitsCodec.encodeRequest(name, increase);
        invokeOnPartition(request);
    }

    @Override
    public void release() {
        ClientMessage request = SemaphoreReleaseCodec.encodeRequest(name, 1);
        invokeOnPartition(request);
    }

    @Override
    public void release(int permits) {
        checkNegative(permits);
        ClientMessage request = SemaphoreReleaseCodec.encodeRequest(name, permits);
        invokeOnPartition(request);
    }

    @Override
    public boolean tryAcquire() {
        ClientMessage request = SemaphoreTryAcquireCodec.encodeRequest(name, 1, 0);
        ClientMessage response = invokeOnPartition(request);
        SemaphoreTryAcquireCodec.ResponseParameters resultParameters = SemaphoreTryAcquireCodec.decodeResponse(response);
        return resultParameters.response;
    }

    @Override
    public boolean tryAcquire(int permits) {
        checkNegative(permits);
        try {
            return tryAcquire(permits, 0, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            currentThread().interrupt();
            return false;
        }
    }

    @Override
    public boolean tryAcquire(long timeout, TimeUnit unit) throws InterruptedException {
        if (timeout == 0) {
            return tryAcquire();
        }
        ClientMessage request = SemaphoreTryAcquireCodec.encodeRequest(name, 1, unit.toMillis(timeout));
        ClientMessage response = invokeOnPartition(request);
        SemaphoreTryAcquireCodec.ResponseParameters resultParameters = SemaphoreTryAcquireCodec.decodeResponse(response);
        return resultParameters.response;
    }

    @Override
    public boolean tryAcquire(int permits, long timeout, TimeUnit unit) throws InterruptedException {
        checkNegative(permits);
        ClientMessage request = SemaphoreTryAcquireCodec.encodeRequest(name, permits, unit.toMillis(timeout));
        ClientMessage response = invokeOnPartition(request);
        SemaphoreTryAcquireCodec.ResponseParameters resultParameters = SemaphoreTryAcquireCodec.decodeResponse(response);
        return resultParameters.response;
    }

    private void checkNegative(int permits) {
        if (permits < 0) {
            throw new IllegalArgumentException("Permits cannot be negative!");
        }
    }

    @Override
    public String toString() {
        return "ISemaphore{" + "name='" + name + '\'' + '}';
    }
}
