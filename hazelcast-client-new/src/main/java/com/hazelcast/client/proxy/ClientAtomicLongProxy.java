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

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.parameters.AtomicLongAddAndGetParameters;
import com.hazelcast.client.impl.protocol.parameters.AtomicLongAlterAndGetParameters;
import com.hazelcast.client.impl.protocol.parameters.AtomicLongAlterParameters;
import com.hazelcast.client.impl.protocol.parameters.AtomicLongApplyParameters;
import com.hazelcast.client.impl.protocol.parameters.AtomicLongCompareAndSetParameters;
import com.hazelcast.client.impl.protocol.parameters.AtomicLongGetAndAddParameters;
import com.hazelcast.client.impl.protocol.parameters.AtomicLongGetAndAlterParameters;
import com.hazelcast.client.impl.protocol.parameters.AtomicLongGetAndSetParameters;
import com.hazelcast.client.impl.protocol.parameters.AtomicLongSetParameters;
import com.hazelcast.client.impl.protocol.parameters.BooleanResultParameters;
import com.hazelcast.client.impl.protocol.parameters.LongResultParameters;
import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IFunction;
import com.hazelcast.nio.serialization.Data;

import static com.hazelcast.util.Preconditions.isNotNull;

public class ClientAtomicLongProxy extends ClientProxy implements IAtomicLong {

    private final String name;
    private volatile Data key;

    public ClientAtomicLongProxy(String serviceName, String objectId) {
        super(serviceName, objectId);
        this.name = objectId;
    }

    @Override
    public <R> R apply(IFunction<Long, R> function) {
        isNotNull(function, "function");
        ClientMessage request = AtomicLongApplyParameters.encode(name, toData(function));
        return invoke(request);
    }

    @Override
    public void alter(IFunction<Long, Long> function) {
        isNotNull(function, "function");
        ClientMessage request = AtomicLongAlterParameters.encode(name, toData(function));
        invoke(request);
    }

    @Override
    public long alterAndGet(IFunction<Long, Long> function) {
        isNotNull(function, "function");
        ClientMessage request = AtomicLongAlterAndGetParameters.encode(name, toData(function));
        LongResultParameters resultParameters = LongResultParameters.decode((ClientMessage) invoke(request));
        return resultParameters.result;
    }

    @Override
    public long getAndAlter(IFunction<Long, Long> function) {
        isNotNull(function, "function");
        ClientMessage request = AtomicLongGetAndAlterParameters.encode(name, toData(function));
        LongResultParameters resultParameters = LongResultParameters.decode((ClientMessage) invoke(request));
        return resultParameters.result;
    }

    @Override
    public long addAndGet(long delta) {
        ClientMessage request = AtomicLongAddAndGetParameters.encode(name, delta);
        LongResultParameters resultParameters = LongResultParameters.decode((ClientMessage) invoke(request));
        return resultParameters.result;
    }

    @Override
    public boolean compareAndSet(long expect, long update) {
        ClientMessage request = AtomicLongCompareAndSetParameters.encode(name, expect, update);
        BooleanResultParameters resultParameters = BooleanResultParameters.decode((ClientMessage) invoke(request));
        return resultParameters.result;
    }

    @Override
    public long decrementAndGet() {
        return addAndGet(-1);
    }

    @Override
    public long get() {
        return getAndAdd(0);
    }

    @Override
    public long getAndAdd(long delta) {
        ClientMessage request = AtomicLongGetAndAddParameters.encode(name, delta);
        LongResultParameters resultParameters = LongResultParameters.decode((ClientMessage) invoke(request));
        return resultParameters.result;
    }

    @Override
    public long getAndSet(long newValue) {
        ClientMessage request = AtomicLongGetAndSetParameters.encode(name, newValue);
        LongResultParameters resultParameters = LongResultParameters.decode((ClientMessage) invoke(request));
        return resultParameters.result;
    }

    @Override
    public long incrementAndGet() {
        return addAndGet(1);
    }

    @Override
    public long getAndIncrement() {
        return getAndAdd(1);
    }

    @Override
    public void set(long newValue) {
        ClientMessage request = AtomicLongSetParameters.encode(name, newValue);
        invoke(request);
    }

    protected <T> T invoke(ClientMessage req) {
        return super.invoke(req, getKey());
    }

    private Data getKey() {
        if (key == null) {
            key = toData(name);
        }
        return key;
    }

    @Override
    public String toString() {
        return "IAtomicLong{" + "name='" + name + '\'' + '}';
    }
}
