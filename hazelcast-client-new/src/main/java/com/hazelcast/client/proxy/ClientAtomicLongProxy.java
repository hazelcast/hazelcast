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
import com.hazelcast.client.impl.protocol.codec.AtomicLongDecrementAndGetCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicLongGetAndAddCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicLongGetAndAlterCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicLongGetAndIncrementCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicLongGetAndSetCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicLongGetCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicLongIncrementAndGetCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicLongSetCodec;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IFunction;

import static com.hazelcast.util.Preconditions.isNotNull;

public class ClientAtomicLongProxy extends PartitionSpecificClientProxy implements IAtomicLong {

    public ClientAtomicLongProxy(String serviceName, String objectId) {
        super(serviceName, objectId);
    }

    @Override
    public <R> R apply(IFunction<Long, R> function) {
        isNotNull(function, "function");
        ClientMessage request = AtomicLongApplyCodec.encodeRequest(name, toData(function));
        ClientMessage response = invokeOnPartition(request);
        AtomicLongApplyCodec.ResponseParameters resultParameters = AtomicLongApplyCodec.decodeResponse(response);
        return toObject(resultParameters.response);
    }

    @Override
    public void alter(IFunction<Long, Long> function) {
        isNotNull(function, "function");
        ClientMessage request = AtomicLongAlterCodec.encodeRequest(name, toData(function));
        invokeOnPartition(request);
    }

    @Override
    public long alterAndGet(IFunction<Long, Long> function) {
        isNotNull(function, "function");
        ClientMessage request = AtomicLongAlterAndGetCodec.encodeRequest(name, toData(function));
        AtomicLongAlterAndGetCodec.ResponseParameters resultParameters
                = AtomicLongAlterAndGetCodec.decodeResponse(invokeOnPartition(request));
        return resultParameters.response;
    }

    @Override
    public long getAndAlter(IFunction<Long, Long> function) {
        isNotNull(function, "function");
        ClientMessage request = AtomicLongGetAndAlterCodec.encodeRequest(name, toData(function));
        AtomicLongGetAndAlterCodec.ResponseParameters resultParameters
                = AtomicLongGetAndAlterCodec.decodeResponse(invokeOnPartition(request));
        return resultParameters.response;
    }

    @Override
    public long addAndGet(long delta) {
        ClientMessage request = AtomicLongAddAndGetCodec.encodeRequest(name, delta);
        AtomicLongAddAndGetCodec.ResponseParameters resultParameters
                = AtomicLongAddAndGetCodec.decodeResponse(invokeOnPartition(request));
        return resultParameters.response;
    }

    @Override
    public boolean compareAndSet(long expect, long update) {
        ClientMessage request = AtomicLongCompareAndSetCodec.encodeRequest(name, expect, update);
        AtomicLongCompareAndSetCodec.ResponseParameters resultParameters
                = AtomicLongCompareAndSetCodec.decodeResponse(invokeOnPartition(request));
        return resultParameters.response;
    }

    @Override
    public long decrementAndGet() {
        ClientMessage request = AtomicLongDecrementAndGetCodec.encodeRequest(name);
        AtomicLongDecrementAndGetCodec.ResponseParameters resultParameters
                = AtomicLongDecrementAndGetCodec.decodeResponse(invokeOnPartition(request));
        return resultParameters.response;
    }

    @Override
    public long get() {
        ClientMessage request = AtomicLongGetCodec.encodeRequest(name);
        AtomicLongGetCodec.ResponseParameters resultParameters
                = AtomicLongGetCodec.decodeResponse(invokeOnPartition(request));
        return resultParameters.response;
    }

    @Override
    public long getAndAdd(long delta) {
        ClientMessage request = AtomicLongGetAndAddCodec.encodeRequest(name, delta);
        AtomicLongGetAndAddCodec.ResponseParameters resultParameters
                = AtomicLongGetAndAddCodec.decodeResponse(invokeOnPartition(request));
        return resultParameters.response;
    }

    @Override
    public long getAndSet(long newValue) {
        ClientMessage request = AtomicLongGetAndSetCodec.encodeRequest(name, newValue);
        AtomicLongGetAndSetCodec.ResponseParameters resultParameters
                = AtomicLongGetAndSetCodec.decodeResponse(invokeOnPartition(request));
        return resultParameters.response;
    }

    @Override
    public long incrementAndGet() {
        ClientMessage request = AtomicLongIncrementAndGetCodec.encodeRequest(name);
        AtomicLongIncrementAndGetCodec.ResponseParameters resultParameters
                = AtomicLongIncrementAndGetCodec.decodeResponse(invokeOnPartition(request));
        return resultParameters.response;
    }

    @Override
    public long getAndIncrement() {
        ClientMessage request = AtomicLongGetAndIncrementCodec.encodeRequest(name);
        AtomicLongGetAndIncrementCodec.ResponseParameters resultParameters
                = AtomicLongGetAndIncrementCodec.decodeResponse(invokeOnPartition(request));
        return resultParameters.response;
    }

    @Override
    public void set(long newValue) {
        ClientMessage request = AtomicLongSetCodec.encodeRequest(name, newValue);
        invokeOnPartition(request);
    }

    @Override
    public String toString() {
        return "IAtomicLong{" + "name='" + name + '\'' + '}';
    }
}
