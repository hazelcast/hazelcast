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
import com.hazelcast.concurrent.atomicreference.client.GetRequest;
import com.hazelcast.concurrent.atomicreference.client.ApplyRequest;
import com.hazelcast.concurrent.atomicreference.client.AlterRequest;
import com.hazelcast.concurrent.atomicreference.client.AlterAndGetRequest;
import com.hazelcast.concurrent.atomicreference.client.GetAndAlterRequest;
import com.hazelcast.concurrent.atomicreference.client.CompareAndSetRequest;
import com.hazelcast.concurrent.atomicreference.client.ContainsRequest;
import com.hazelcast.concurrent.atomicreference.client.SetRequest;
import com.hazelcast.concurrent.atomicreference.client.GetAndSetRequest;
import com.hazelcast.concurrent.atomicreference.client.IsNullRequest;

import com.hazelcast.core.IFunction;
import com.hazelcast.core.IAtomicReference;
import com.hazelcast.nio.serialization.Data;

import static com.hazelcast.util.ValidationUtil.isNotNull;

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
        ClientMessage request = AtomicReferenceApplyParameters.encode(name, toData(function));
        GenericResultParameters resultParameters = GenericResultParameters.decode((ClientMessage)invoke(request));
        return toObject(resultParameters.result);
    }

    @Override
    public void alter(IFunction<E, E> function) {
        isNotNull(function, "function");
        ClientMessage request = AtomicReferenceAlterParameters.encode(name, toData(function));
        invoke(request);
    }

    @Override
    public E alterAndGet(IFunction<E, E> function) {
        isNotNull(function, "function");
        ClientMessage request = AtomicReferenceAlterAndGetParameters.encode(name, toData(function));
        GenericResultParameters resultParameters = GenericResultParameters.decode((ClientMessage)invoke(request));
        return toObject(resultParameters.result);
    }

    @Override
    public E getAndAlter(IFunction<E, E> function) {
        isNotNull(function, "function");
        ClientMessage request = AtomicReferenceGetAndAlterParameters.encode(name, toData(function));
        GenericResultParameters resultParameters = GenericResultParameters.decode((ClientMessage)invoke(request));
        return toObject(resultParameters.result);

    }

    @Override
    public boolean compareAndSet(E expect, E update) {
        ClientMessage request = AtomicReferenceCompareAndSetParameters.encode(name, toData(expect), toData(update));
        BooleanResultParameters resultParameters = BooleanResultParameters.decode((ClientMessage)invoke(request));
        return resultParameters.result;
    }

    @Override
    public boolean contains(E expected) {
        ClientMessage request = AtomicReferenceContainsParameters.encode(name, toData(expected));
        BooleanResultParameters resultParameters = BooleanResultParameters.decode((ClientMessage)invoke(request));
        return resultParameters.result;    }

    @Override
    public E get() {
        ClientMessage request = AtomicReferenceGetParameters.encode(name);
        GenericResultParameters resultParameters = GenericResultParameters.decode((ClientMessage)invoke(request));
        return toObject(resultParameters.result);
    }

    @Override
    public void set(E newValue) {
        ClientMessage request = AtomicReferenceSetParameters.encode(name, toData(newValue));
        invoke(request);
    }

    @Override
    public void clear() {
        ClientMessage request = AtomicReferenceClearParameters.encode(name);
        invoke(request);
    }

    @Override
    public E getAndSet(E newValue) {
        ClientMessage request = AtomicReferenceGetAndSetParameters.encode(name, toData(newValue));
        GenericResultParameters resultParameters = GenericResultParameters.decode((ClientMessage)invoke(request));
        return toObject(resultParameters.result);
    }

    @Override
    public E setAndGet(E update) {
        ClientMessage request = AtomicReferenceSetAndGetParameters.encode(name, toData(update));
        GenericResultParameters resultParameters = GenericResultParameters.decode((ClientMessage)invoke(request));
        return toObject(resultParameters.result);
    }

    @Override
    public boolean isNull() {
        ClientMessage request = AtomicReferenceIsNullParameters.encode(name);
        BooleanResultParameters resultParameters = BooleanResultParameters.decode((ClientMessage)invoke(request));
        return resultParameters.result;    }

    protected <T> T invoke(ClientRequest req) {
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

