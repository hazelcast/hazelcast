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
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.util.ExceptionUtil;

/**
 * @author ali 5/24/13
 */
public class ClientAtomicLongProxy extends ClientProxy implements IAtomicLong {

    private final String name;
    private Data key;

    public ClientAtomicLongProxy(String serviceName, String objectId) {
        super(serviceName, objectId);
        this.name = objectId;
    }

    public long addAndGet(long delta) {
        AddAndGetRequest request = new AddAndGetRequest(name, delta);
        Long result = invoke(request);
        return result;
    }

    public boolean compareAndSet(long expect, long update) {
        CompareAndSetRequest request = new CompareAndSetRequest(name, expect, update);
        Boolean result =invoke(request);
        return result;
    }

    public long decrementAndGet() {
        return addAndGet(-1);
    }

    public long get() {
        return getAndAdd(0);
    }

    public long getAndAdd(long delta) {
        GetAndAddRequest request = new GetAndAddRequest(name, delta);
        Long result = invoke(request);
        return result;
    }

    public long getAndSet(long newValue) {
        GetAndSetRequest request = new GetAndSetRequest(name, newValue);
        Long result = invoke(request);
        return result;
    }

    public long incrementAndGet() {
        return addAndGet(1);
    }

    public long getAndIncrement() {
        return getAndAdd(1);
    }

    public void set(long newValue) {
        SetRequest request = new SetRequest(name, newValue);
        invoke(request);
    }

    protected void onDestroy() {
        AtomicLongDestroyRequest request = new AtomicLongDestroyRequest(name);
        invoke(request);
    }

    public String getName() {
        return name;
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
            key = getContext().getSerializationService().toData(name);
        }
        return key;
    }
}
