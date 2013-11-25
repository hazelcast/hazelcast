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
import com.hazelcast.concurrent.atomiclong.client.*;
import com.hazelcast.concurrent.atomiclong.client.AlterAndGetRequest;
import com.hazelcast.concurrent.atomiclong.client.AlterRequest;
import com.hazelcast.concurrent.atomiclong.client.ApplyRequest;
import com.hazelcast.concurrent.atomiclong.client.CompareAndSetRequest;
import com.hazelcast.concurrent.atomiclong.client.GetAndAlterRequest;
import com.hazelcast.concurrent.atomiclong.client.GetAndSetRequest;
import com.hazelcast.concurrent.atomiclong.client.SetRequest;
import com.hazelcast.concurrent.atomicreference.client.*;
import com.hazelcast.core.Function;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.util.ExceptionUtil;

import static com.hazelcast.util.ValidationUtil.isNotNull;

/**
 * @author ali 5/24/13
 */
public class ClientAtomicLongProxy extends ClientProxy implements IAtomicLong {

    private final String name;
    private volatile Data key;

    public ClientAtomicLongProxy(String serviceName, String objectId) {
        super(serviceName, objectId);
        this.name = objectId;
    }

    @Override
    public <R> R apply(Function<Long, R> function) {
        isNotNull(function, "function");
        return invoke(new ApplyRequest(name, toData(function)));
    }

    @Override
    public void alter(Function<Long, Long> function) {
        isNotNull(function, "function");
        invoke(new AlterRequest(name, toData(function)));
    }

    @Override
    public long alterAndGet(Function<Long, Long> function) {
        isNotNull(function, "function");
        return (Long)invoke(new AlterAndGetRequest(name, toData(function)));
    }

    @Override
    public long getAndAlter(Function<Long, Long> function) {
        isNotNull(function, "function");
        return (Long)invoke(new GetAndAlterRequest(name, toData(function)));
    }

    @Override
    public long addAndGet(long delta) {
        AddAndGetRequest request = new AddAndGetRequest(name, delta);
        Long result = invoke(request);
        return result;
    }

    @Override
    public boolean compareAndSet(long expect, long update) {
        CompareAndSetRequest request = new CompareAndSetRequest(name, expect, update);
        Boolean result =invoke(request);
        return result;
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
        GetAndAddRequest request = new GetAndAddRequest(name, delta);
        Long result = invoke(request);
        return result;
    }

    @Override
    public long getAndSet(long newValue) {
        GetAndSetRequest request = new GetAndSetRequest(name, newValue);
        Long result = invoke(request);
        return result;
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
        SetRequest request = new SetRequest(name, newValue);
        invoke(request);
    }

    @Override
    protected void onDestroy() {
    }

    private <T> T invoke(Object req){
        try {
            return getContext().getInvocationService().invokeOnKeyOwner(req, getKey());
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    private Data getKey(){
        if (key == null){
            key = toData(name);
        }
        return key;
    }

    private Data toData(Object object) {
        return getContext().getSerializationService().toData(object);
    }
}
