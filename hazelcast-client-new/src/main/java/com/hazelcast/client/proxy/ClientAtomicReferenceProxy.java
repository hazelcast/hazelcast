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
import com.hazelcast.core.IAtomicReference;
import com.hazelcast.core.IFunction;

import static com.hazelcast.util.Preconditions.isNotNull;

public class ClientAtomicReferenceProxy<E> extends PartitionSpecificClientProxy implements IAtomicReference<E> {

    public ClientAtomicReferenceProxy(String serviceName, String objectId) {
        super(serviceName, objectId);
    }

    @Override
    public <R> R apply(IFunction<E, R> function) {
        isNotNull(function, "function");
        ClientMessage request = AtomicReferenceApplyCodec.encodeRequest(name, toData(function));
        ClientMessage response = invokeOnPartition(request);
        AtomicReferenceApplyCodec.ResponseParameters resultParameters =
                AtomicReferenceApplyCodec.decodeResponse(response);
        return toObject(resultParameters.response);
    }

    @Override
    public void alter(IFunction<E, E> function) {
        isNotNull(function, "function");
        ClientMessage request = AtomicReferenceAlterCodec.encodeRequest(name, toData(function));
        invokeOnPartition(request);
    }

    @Override
    public E alterAndGet(IFunction<E, E> function) {
        isNotNull(function, "function");
        ClientMessage request = AtomicReferenceAlterAndGetCodec.encodeRequest(name, toData(function));
        ClientMessage response = invokeOnPartition(request);
        return toObject(AtomicReferenceAlterAndGetCodec.decodeResponse(response).response);
    }

    @Override
    public E getAndAlter(IFunction<E, E> function) {
        isNotNull(function, "function");
        ClientMessage request = AtomicReferenceGetAndAlterCodec.encodeRequest(name, toData(function));
        ClientMessage response = invokeOnPartition(request);
        return toObject(AtomicReferenceGetAndAlterCodec.decodeResponse(response).response);

    }

    @Override
    public boolean compareAndSet(E expect, E update) {
        ClientMessage request = AtomicReferenceCompareAndSetCodec.encodeRequest(name, toData(expect), toData(update));
        ClientMessage response = invokeOnPartition(request);
        return AtomicReferenceCompareAndSetCodec.decodeResponse(response).response;
    }

    @Override
    public boolean contains(E expected) {
        ClientMessage request = AtomicReferenceContainsCodec.encodeRequest(name, toData(expected));
        ClientMessage response = invokeOnPartition(request);
        return AtomicReferenceContainsCodec.decodeResponse(response).response;
    }

    @Override
    public E get() {
        ClientMessage request = AtomicReferenceGetCodec.encodeRequest(name);
        ClientMessage response = invokeOnPartition(request);
        return toObject(AtomicReferenceGetCodec.decodeResponse(response).response);
    }

    @Override
    public void set(E newValue) {
        ClientMessage request = AtomicReferenceSetCodec.encodeRequest(name, toData(newValue));
        invokeOnPartition(request);
    }

    @Override
    public void clear() {
        ClientMessage request = AtomicReferenceClearCodec.encodeRequest(name);
        invokeOnPartition(request);
    }

    @Override
    public E getAndSet(E newValue) {
        ClientMessage request = AtomicReferenceGetAndSetCodec.encodeRequest(name, toData(newValue));
        ClientMessage response = invokeOnPartition(request);
        return toObject(AtomicReferenceGetAndSetCodec.decodeResponse(response).response);
    }

    @Override
    public E setAndGet(E update) {
        ClientMessage request = AtomicReferenceSetAndGetCodec.encodeRequest(name, toData(update));
        ClientMessage response = invokeOnPartition(request);
        return toObject(AtomicReferenceSetAndGetCodec.decodeResponse(response).response);
    }

    @Override
    public boolean isNull() {
        ClientMessage request = AtomicReferenceIsNullCodec.encodeRequest(name);
        ClientMessage response = invokeOnPartition(request);
        return AtomicReferenceIsNullCodec.decodeResponse(response).response;
    }

    @Override
    public String toString() {
        return "IAtomicReference{" + "name='" + name + '\'' + '}';
    }

}

