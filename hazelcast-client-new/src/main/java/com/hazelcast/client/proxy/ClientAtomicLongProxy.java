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
import com.hazelcast.client.impl.protocol.codec.AtomicLongAddAndGetCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicLongAlterAndGetCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicLongAlterCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicLongApplyCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicLongCompareAndSetCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicLongGetAndAddCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicLongGetAndAlterCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicLongGetAndSetCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicLongSetCodec;
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
        ClientMessage request = AtomicLongApplyCodec.encodeRequest(name, toData(function));
        ClientMessage response = invoke(request);
        AtomicLongApplyCodec.ResponseParameters resultParameters = AtomicLongApplyCodec.decodeResponse(response);
        return toObject(resultParameters.response);
    }

    @Override
    public void alter(IFunction<Long, Long> function) {
        isNotNull(function, "function");
        ClientMessage request = AtomicLongAlterCodec.encodeRequest(name, toData(function));
        invoke(request);
    }

    @Override
    public long alterAndGet(IFunction<Long, Long> function) {
        isNotNull(function, "function");
        ClientMessage request = AtomicLongAlterAndGetCodec.encodeRequest(name, toData(function));
        AtomicLongAlterAndGetCodec.ResponseParameters resultParameters
                = AtomicLongAlterAndGetCodec.decodeResponse(invokeMessage(request));
        return resultParameters.response;
    }

    @Override
    public long getAndAlter(IFunction<Long, Long> function) {
        isNotNull(function, "function");
        ClientMessage request = AtomicLongGetAndAlterCodec.encodeRequest(name, toData(function));
        AtomicLongGetAndAlterCodec.ResponseParameters resultParameters
                = AtomicLongGetAndAlterCodec.decodeResponse(invokeMessage(request));
        return resultParameters.response;
    }

    @Override
    public long addAndGet(long delta) {
        ClientMessage request = AtomicLongAddAndGetCodec.encodeRequest(name, delta);
        AtomicLongAddAndGetCodec.ResponseParameters resultParameters
                = AtomicLongAddAndGetCodec.decodeResponse(invokeMessage(request));
        return resultParameters.response;
    }

    @Override
    public boolean compareAndSet(long expect, long update) {
        ClientMessage request = AtomicLongCompareAndSetCodec.encodeRequest(name, expect, update);
        AtomicLongCompareAndSetCodec.ResponseParameters resultParameters
                = AtomicLongCompareAndSetCodec.decodeResponse(invokeMessage(request));
        return resultParameters.response;
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
        ClientMessage request = AtomicLongGetAndAddCodec.encodeRequest(name, delta);
        AtomicLongGetAndAddCodec.ResponseParameters resultParameters
                = AtomicLongGetAndAddCodec.decodeResponse(invokeMessage(request));
        return resultParameters.response;
    }

    @Override
    public long getAndSet(long newValue) {
        ClientMessage request = AtomicLongGetAndSetCodec.encodeRequest(name, newValue);
        AtomicLongGetAndSetCodec.ResponseParameters resultParameters
                = AtomicLongGetAndSetCodec.decodeResponse(invokeMessage(request));
        return resultParameters.response;
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
        ClientMessage request = AtomicLongSetCodec.encodeRequest(name, newValue);
        invokeMessage(request);
    }

    protected ClientMessage invokeMessage(ClientMessage req) {
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
