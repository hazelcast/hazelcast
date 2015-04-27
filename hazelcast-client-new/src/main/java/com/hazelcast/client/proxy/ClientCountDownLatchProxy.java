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
import com.hazelcast.client.impl.protocol.parameters.BooleanResultParameters;
import com.hazelcast.client.impl.protocol.parameters.CountDownLatchAwaitParameters;
import com.hazelcast.client.impl.protocol.parameters.CountDownLatchCountDownParameters;
import com.hazelcast.client.impl.protocol.parameters.CountDownLatchGetCountParameters;
import com.hazelcast.client.impl.protocol.parameters.CountDownLatchTrySetCountParameters;
import com.hazelcast.client.impl.protocol.parameters.IntResultParameters;
import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.core.ICountDownLatch;
import com.hazelcast.nio.serialization.Data;

import java.util.concurrent.TimeUnit;

public class ClientCountDownLatchProxy extends ClientProxy implements ICountDownLatch {

    private volatile Data key;

    public ClientCountDownLatchProxy(String serviceName, String objectId) {
        super(serviceName, objectId);
    }

    public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
        ClientMessage request = CountDownLatchAwaitParameters.encode(getName(), getTimeInMillis(timeout, unit));
        BooleanResultParameters resultParameters = BooleanResultParameters.decode((ClientMessage) invoke(request));
        return resultParameters.result;
    }

    public void countDown() {
        ClientMessage request = CountDownLatchCountDownParameters.encode(getName());
        invoke(request);
    }

    public int getCount() {
        ClientMessage request = CountDownLatchGetCountParameters.encode(getName());
        IntResultParameters resultParameters = IntResultParameters.decode((ClientMessage) invoke(request));
        return resultParameters.result;
    }

    public boolean trySetCount(int count) {
        if (count < 0) {
            throw new IllegalArgumentException("count can't be negative");
        }
        ClientMessage request = CountDownLatchTrySetCountParameters.encode(getName(), count);
        BooleanResultParameters resultParameters = BooleanResultParameters.decode((ClientMessage) invoke(request));
        return resultParameters.result;
    }

    private Data getKey() {
        if (key == null) {
            key = toData(getName());
        }
        return key;
    }

    private long getTimeInMillis(final long time, final TimeUnit timeunit) {
        return timeunit != null ? timeunit.toMillis(time) : time;
    }

    protected <T> T invoke(ClientMessage req) {
        return super.invoke(req, getKey());
    }

    @Override
    public String toString() {
        return "ICountDownLatch{" + "name='" + getName() + '\'' + '}';
    }
}
