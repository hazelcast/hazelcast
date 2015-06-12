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

import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.client.spi.impl.ClientInvocationFuture;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IFunction;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.ringbuffer.OverflowPolicy;
import com.hazelcast.ringbuffer.ReadResultSet;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.ringbuffer.impl.client.AddAllRequest;
import com.hazelcast.ringbuffer.impl.client.AddAsyncRequest;
import com.hazelcast.ringbuffer.impl.client.AddRequest;
import com.hazelcast.ringbuffer.impl.client.CapacityRequest;
import com.hazelcast.ringbuffer.impl.client.HeadSequenceRequest;
import com.hazelcast.ringbuffer.impl.client.ReadManyRequest;
import com.hazelcast.ringbuffer.impl.client.ReadOneRequest;
import com.hazelcast.ringbuffer.impl.client.RemainingCapacityRequest;
import com.hazelcast.ringbuffer.impl.client.SizeRequest;
import com.hazelcast.ringbuffer.impl.client.TailSequenceRequest;

import java.util.Collection;

import static com.hazelcast.ringbuffer.impl.RingbufferProxy.MAX_BATCH_SIZE;
import static com.hazelcast.util.Preconditions.checkFalse;
import static com.hazelcast.util.Preconditions.checkNotNegative;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static com.hazelcast.util.Preconditions.checkTrue;

public class ClientRingbufferProxy<E> extends ClientProxy implements Ringbuffer<E> {

    private volatile long capacity = -1;

    public ClientRingbufferProxy(String serviceName, String objectName) {
        super(serviceName, objectName);
    }

    @Override
    public long capacity() {
        if (capacity == -1) {
            CapacityRequest request = new CapacityRequest(getName());
            capacity = invoke(request);
        }

        return capacity;
    }

    @Override
    public long size() {
        SizeRequest request = new SizeRequest(getName());
        Long result = invoke(request);
        return result;
    }

    @Override
    public long tailSequence() {
        TailSequenceRequest request = new TailSequenceRequest(getName());
        Long result = invoke(request);
        return result;
    }

    @Override
    public long headSequence() {
        HeadSequenceRequest request = new HeadSequenceRequest(getName());
        Long result = invoke(request);
        return result;
    }

    @Override
    public long remainingCapacity() {
        RemainingCapacityRequest request = new RemainingCapacityRequest(getName());
        Long result = invoke(request);
        return result;
    }

    @Override
    public long add(E item) {
        checkNotNull(item, "item can't be null");

        AddRequest request = new AddRequest(getName(), toData(item));
        Long result = invoke(request);
        return result;
    }

    @Override
    public ICompletableFuture<Long> addAsync(E item, OverflowPolicy overflowPolicy) {
        checkNotNull(item, "item can't be null");
        checkNotNull(overflowPolicy, "overflowPolicy can't be null");

        AddAsyncRequest request = new AddAsyncRequest(getName(), toData(item), overflowPolicy);
        ClientInvocationFuture f  = new ClientInvocation(getClient(), request).invoke();
        f.setResponseDeserialized(true);
        return f;
    }

    @Override
    public E readOne(long sequence) throws InterruptedException {
        checkSequence(sequence);

        ReadOneRequest request = new ReadOneRequest(getName(), sequence);
        E result = invoke(request);
        return result;
    }

    @Override
    public ICompletableFuture<Long> addAllAsync(Collection<? extends E> collection, OverflowPolicy overflowPolicy) {
        checkNotNull(collection, "collection can't be null");
        checkNotNull(overflowPolicy, "overflowPolicy can't be null");
        checkFalse(collection.isEmpty(), "collection can't be empty");
        checkTrue(collection.size() <= MAX_BATCH_SIZE, "collection can't be larger than " + MAX_BATCH_SIZE);

        AddAllRequest request = new AddAllRequest(getName(), toDataArray(collection), overflowPolicy);
        ClientInvocationFuture f = new ClientInvocation(getClient(), request).invoke();
        f.setResponseDeserialized(true);
        return f;
    }

    @Override
    public ICompletableFuture<ReadResultSet<E>> readManyAsync(long startSequence, int minCount,
                                                              int maxCount, IFunction<E, Boolean> filter) {

        checkSequence(startSequence);
        checkNotNegative(minCount, "minCount can't be smaller than 0");
        checkTrue(maxCount >= minCount, "maxCount should be equal or larger than minCount");
        checkTrue(minCount <= capacity(), "the minCount should be smaller than or equal to the capacity");
        checkTrue(maxCount <= MAX_BATCH_SIZE, "maxCount can't be larger than " + MAX_BATCH_SIZE);

        ReadManyRequest request = new ReadManyRequest(getName(), startSequence, minCount, maxCount, toData(filter));
        ClientInvocationFuture f = new ClientInvocation(getClient(), request).invoke();
        f.setResponseDeserialized(true);
        return f;
    }

    private Data[] toDataArray(Collection<? extends E> collection) {
        Data[] items = new Data[collection.size()];
        int k = 0;
        for (E item : collection) {
            checkNotNull(item, "collection can't contains null items");
            items[k] = toData(item);
            k++;
        }
        return items;
    }

    private static void checkSequence(long sequence) {
        if (sequence < 0) {
            throw new IllegalArgumentException("sequence can't be smaller than 0, but was: " + sequence);
        }
    }
}
