/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.impl.protocol.codec.CountDownLatchAwaitCodec;
import com.hazelcast.client.impl.protocol.codec.CountDownLatchCountDownCodec;
import com.hazelcast.client.impl.protocol.codec.CountDownLatchGetCountCodec;
import com.hazelcast.client.impl.protocol.codec.CountDownLatchTrySetCountCodec;
import com.hazelcast.core.ICountDownLatch;

import java.util.concurrent.TimeUnit;

/**
 * Proxy implementation of {@link ICountDownLatch}.
 */
public class ClientCountDownLatchProxy extends PartitionSpecificClientProxy implements ICountDownLatch {

    public ClientCountDownLatchProxy(String serviceName, String objectId) {
        super(serviceName, objectId);
    }

    @Override
    public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
        ClientMessage request = CountDownLatchAwaitCodec.encodeRequest(name, getTimeInMillis(timeout, unit));
        CountDownLatchAwaitCodec.ResponseParameters resultParameters =
                CountDownLatchAwaitCodec.decodeResponse(invokeOnPartition(request));
        return resultParameters.response;
    }

    @Override
    public void countDown() {
        ClientMessage request = CountDownLatchCountDownCodec.encodeRequest(name);
        invokeOnPartition(request);
    }

    @Override
    public int getCount() {
        ClientMessage request = CountDownLatchGetCountCodec.encodeRequest(name);
        CountDownLatchGetCountCodec.ResponseParameters resultParameters =
                CountDownLatchGetCountCodec.decodeResponse(invokeOnPartition(request));
        return resultParameters.response;
    }

    @Override
    public boolean trySetCount(int count) {
        if (count < 0) {
            throw new IllegalArgumentException("count can't be negative");
        }
        ClientMessage request = CountDownLatchTrySetCountCodec.encodeRequest(name, count);
        CountDownLatchTrySetCountCodec.ResponseParameters resultParameters =
                CountDownLatchTrySetCountCodec.decodeResponse(invokeOnPartition(request));
        return resultParameters.response;
    }

    private long getTimeInMillis(final long time, final TimeUnit timeunit) {
        return timeunit != null ? timeunit.toMillis(time) : time;
    }


    @Override
    public String toString() {
        return "ICountDownLatch{" + "name='" + name + '\'' + '}';
    }
}
