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
import com.hazelcast.client.impl.protocol.codec.AtomicReferenceAlterAndGetCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicReferenceAlterCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicReferenceApplyCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicReferenceClearCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicReferenceCompareAndSetCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicReferenceContainsCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicReferenceGetAndAlterCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicReferenceGetAndSetCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicReferenceGetCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicReferenceIsNullCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicReferenceSetAndGetCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicReferenceSetCodec;
import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.core.IAtomicReference;
import com.hazelcast.core.IFunction;
import com.hazelcast.nio.serialization.Data;

import static com.hazelcast.util.Preconditions.isNotNull;

public class ClientAtomicReferenceProxy<E> extends ClientProxy implements IAtomicReference<E> {

    private final String name;
    private volatile Data key;

    public ClientAtomicReferenceProxy(String serviceName, String objectId) {
        super(serviceName, objectId);
        this.name = objectId;
    }

    @Override
    public <R> R apply(IFunction<E, R> function) {
        isNotNull(function, "function");
        ClientMessage request = AtomicReferenceApplyCodec.encodeRequest(name, toData(function));
        AtomicReferenceApplyCodec.ResponseParameters resultParameters =
                AtomicReferenceApplyCodec.decodeResponse((ClientMessage) invoke(request));
        return toObject(resultParameters.response);
    }

    @Override
    public void alter(IFunction<E, E> function) {
        isNotNull(function, "function");
        ClientMessage request = AtomicReferenceAlterCodec.encodeRequest(name, toData(function));
        invoke(request);
    }

    @Override
    public E alterAndGet(IFunction<E, E> function) {
        isNotNull(function, "function");
        ClientMessage request = AtomicReferenceAlterAndGetCodec.encodeRequest(name, toData(function));
        AtomicReferenceAlterAndGetCodec.ResponseParameters resultParameters = AtomicReferenceAlterAndGetCodec.decodeResponse((ClientMessage) invoke(request));
        return toObject(resultParameters.response);
    }

    @Override
    public E getAndAlter(IFunction<E, E> function) {
        isNotNull(function, "function");
        ClientMessage request = AtomicReferenceGetAndAlterCodec.encodeRequest(name, toData(function));
        AtomicReferenceGetAndAlterCodec.ResponseParameters resultParameters = AtomicReferenceGetAndAlterCodec.decodeResponse((ClientMessage) invoke(request));
        return toObject(resultParameters.response);

    }

    @Override
    public boolean compareAndSet(E expect, E update) {
        ClientMessage request = AtomicReferenceCompareAndSetCodec.encodeRequest(name, toData(expect), toData(update));
        AtomicReferenceCompareAndSetCodec.ResponseParameters resultParameters = AtomicReferenceCompareAndSetCodec.decodeResponse((ClientMessage) invoke(request));
        return resultParameters.response;
    }

    @Override
    public boolean contains(E expected) {
        ClientMessage request = AtomicReferenceContainsCodec.encodeRequest(name, toData(expected));
        AtomicReferenceContainsCodec.ResponseParameters resultParameters = AtomicReferenceContainsCodec.decodeResponse((ClientMessage) invoke(request));
        return resultParameters.response;
    }

    @Override
    public E get() {
        ClientMessage request = AtomicReferenceGetCodec.encodeRequest(name);
        AtomicReferenceGetCodec.ResponseParameters resultParameters = AtomicReferenceGetCodec.decodeResponse((ClientMessage) invoke(request));
        return toObject(resultParameters.response);
    }

    @Override
    public void set(E newValue) {
        ClientMessage request = AtomicReferenceSetCodec.encodeRequest(name, toData(newValue));
        invoke(request);
    }

    @Override
    public void clear() {
        ClientMessage request = AtomicReferenceClearCodec.encodeRequest(name);
        invoke(request);
    }

    @Override
    public E getAndSet(E newValue) {
        ClientMessage request = AtomicReferenceGetAndSetCodec.encodeRequest(name, toData(newValue));
        AtomicReferenceGetAndSetCodec.ResponseParameters resultParameters = AtomicReferenceGetAndSetCodec.decodeResponse((ClientMessage) invoke(request));
        return toObject(resultParameters.response);
    }

    @Override
    public E setAndGet(E update) {
        ClientMessage request = AtomicReferenceSetAndGetCodec.encodeRequest(name, toData(update));
        AtomicReferenceSetAndGetCodec.ResponseParameters resultParameters = AtomicReferenceSetAndGetCodec.decodeResponse((ClientMessage) invoke(request));
        return toObject(resultParameters.response);
    }

    @Override
    public boolean isNull() {
        ClientMessage request = AtomicReferenceIsNullCodec.encodeRequest(name);
        AtomicReferenceIsNullCodec.ResponseParameters resultParameters = AtomicReferenceIsNullCodec.decodeResponse((ClientMessage) invoke(request));
        return resultParameters.response;
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
        return "IAtomicReference{" + "name='" + name + '\'' + '}';
    }

}

