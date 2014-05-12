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

import com.hazelcast.client.ClientRequest;
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

    public ClientAtomicReferenceProxy(String instanceName, String serviceName, String objectId) {
        super(instanceName, serviceName, objectId);
        this.name = objectId;
    }

    @Override
    public <R> R apply(IFunction<E, R> function) {
        isNotNull(function, "function");
        return invoke(new ApplyRequest(name, toData(function)));
    }

    @Override
    public void alter(IFunction<E, E> function) {
        isNotNull(function, "function");
        invoke(new AlterRequest(name, toData(function)));
    }

    @Override
    public E alterAndGet(IFunction<E, E> function) {
        isNotNull(function, "function");
        return invoke(new AlterAndGetRequest(name, toData(function)));
    }

    @Override
    public E getAndAlter(IFunction<E, E> function) {
        isNotNull(function, "function");
        return invoke(new GetAndAlterRequest(name, toData(function)));
    }

    @Override
    public boolean compareAndSet(E expect, E update) {
        return (Boolean) invoke(new CompareAndSetRequest(name, toData(expect), toData(update)));
    }

    @Override
    public boolean contains(E expected) {
        return (Boolean) invoke(new ContainsRequest(name, toData(expected)));
    }

    @Override
    public E get() {
        return invoke(new GetRequest(name));
    }

    @Override
    public void set(E newValue) {
        invoke(new SetRequest(name, toData(newValue)));
    }

    @Override
    public void clear() {
        set(null);
    }

    @Override
    public E getAndSet(E newValue) {
        return invoke(new GetAndSetRequest(name, toData(newValue)));
    }

    @Override
    public E setAndGet(E update) {
        invoke(new SetRequest(name, toData(update)));
        return update;
    }

    @Override
    public boolean isNull() {
        return (Boolean) invoke(new IsNullRequest(name));
    }

    @Override
    protected void onDestroy() {
    }

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

