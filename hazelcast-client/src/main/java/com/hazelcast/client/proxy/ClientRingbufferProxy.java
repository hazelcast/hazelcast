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

import com.hazelcast.client.impl.ClientMessageDecoder;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.RingbufferAddAllAsyncCodec;
import com.hazelcast.client.impl.protocol.codec.RingbufferAddAsyncCodec;
import com.hazelcast.client.impl.protocol.codec.RingbufferAddCodec;
import com.hazelcast.client.impl.protocol.codec.RingbufferCapacityCodec;
import com.hazelcast.client.impl.protocol.codec.RingbufferHeadSequenceCodec;
import com.hazelcast.client.impl.protocol.codec.RingbufferReadManyAsyncCodec;
import com.hazelcast.client.impl.protocol.codec.RingbufferReadOneCodec;
import com.hazelcast.client.impl.protocol.codec.RingbufferRemainingCapacityCodec;
import com.hazelcast.client.impl.protocol.codec.RingbufferSizeCodec;
import com.hazelcast.client.impl.protocol.codec.RingbufferTailSequenceCodec;
import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.client.spi.impl.ClientInvocationFuture;
import com.hazelcast.client.util.ClientDelegatingFuture;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IFunction;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.ringbuffer.OverflowPolicy;
import com.hazelcast.ringbuffer.ReadResultSet;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.ringbuffer.impl.client.PortableReadResultSet;
import com.hazelcast.util.ExceptionUtil;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Future;

import static com.hazelcast.ringbuffer.impl.RingbufferProxy.MAX_BATCH_SIZE;
import static com.hazelcast.util.Preconditions.checkFalse;
import static com.hazelcast.util.Preconditions.checkNotNegative;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static com.hazelcast.util.Preconditions.checkTrue;

public class ClientRingbufferProxy<E> extends ClientProxy implements Ringbuffer<E> {

    private static final ClientMessageDecoder ADD_ASYNC_ASYNC_RESPONSE_DECODER = new ClientMessageDecoder() {
        @Override
        public Long decodeClientMessage(ClientMessage clientMessage) {
            return RingbufferAddCodec.decodeResponse(clientMessage).response;
        }
    };

    private static final ClientMessageDecoder ADD_ALL_ASYNC_RESPONSE_DECODER = new ClientMessageDecoder() {
        @Override
        public Long decodeClientMessage(ClientMessage clientMessage) {
            return RingbufferAddAllAsyncCodec.decodeResponse(clientMessage).response;
        }
    };

    private ClientMessageDecoder readManyAsyncResponseDecoder;

    private int partitionId;
    private volatile long capacity = -1;

    public ClientRingbufferProxy(String serviceName, String objectName) {
        super(serviceName, objectName);
    }

    @Override
    protected void onInitialize() {
        partitionId = getContext().getPartitionService().getPartitionId(name);
        final SerializationService serializationService = getContext().getSerializationService();

        readManyAsyncResponseDecoder = new ClientMessageDecoder() {
            @Override
            public PortableReadResultSet decodeClientMessage(ClientMessage clientMessage) {
                final RingbufferReadManyAsyncCodec.ResponseParameters responseParameters
                        = RingbufferReadManyAsyncCodec.decodeResponse(clientMessage);
                PortableReadResultSet readResultSet
                        = new PortableReadResultSet(responseParameters.readCount, responseParameters.items);
                readResultSet.setSerializationService(serializationService);
                return readResultSet;
            }
        };
    }

    @Override
    public long capacity() {
        if (capacity == -1) {
            ClientMessage request = RingbufferCapacityCodec.encodeRequest(name);
            ClientMessage response = invoke(request, partitionId);
            RingbufferCapacityCodec.ResponseParameters resultParameters = RingbufferCapacityCodec.decodeResponse(response);
            capacity = resultParameters.response;
        }

        return capacity;
    }

    @Override
    public long size() {
        ClientMessage request = RingbufferSizeCodec.encodeRequest(name);
        ClientMessage response = invoke(request, partitionId);
        RingbufferSizeCodec.ResponseParameters resultParameters = RingbufferSizeCodec.decodeResponse(response);
        return resultParameters.response;
    }

    @Override
    public long tailSequence() {
        ClientMessage request = RingbufferTailSequenceCodec.encodeRequest(name);
        ClientMessage response = invoke(request, partitionId);
        RingbufferTailSequenceCodec.ResponseParameters resultParameters = RingbufferTailSequenceCodec.decodeResponse(response);
        return resultParameters.response;
    }

    @Override
    public long headSequence() {
        ClientMessage request = RingbufferHeadSequenceCodec.encodeRequest(name);
        ClientMessage response = invoke(request, partitionId);
        RingbufferHeadSequenceCodec.ResponseParameters resultParameters = RingbufferHeadSequenceCodec.decodeResponse(response);
        return resultParameters.response;
    }

    @Override
    public long remainingCapacity() {
        ClientMessage request = RingbufferRemainingCapacityCodec.encodeRequest(name);
        ClientMessage response = invoke(request, partitionId);
        RingbufferRemainingCapacityCodec.ResponseParameters resultParameters
                = RingbufferRemainingCapacityCodec.decodeResponse(response);
        return resultParameters.response;
    }

    @Override
    public long add(E item) {
        checkNotNull(item, "item can't be null");

        Data element = toData(item);
        ClientMessage request = RingbufferAddCodec.encodeRequest(name, OverflowPolicy.OVERWRITE.getId(), element);
        ClientMessage response = invoke(request, partitionId);
        RingbufferAddCodec.ResponseParameters resultParameters = RingbufferAddCodec.decodeResponse(response);
        return resultParameters.response;
    }

    @Override
    public ICompletableFuture<Long> addAsync(E item, OverflowPolicy overflowPolicy) {
        checkNotNull(item, "item can't be null");
        checkNotNull(overflowPolicy, "overflowPolicy can't be null");

        Data element = toData(item);
        ClientMessage request = RingbufferAddAsyncCodec.encodeRequest(name, overflowPolicy.getId(), element);
        request.setPartitionId(partitionId);

        try {
            ClientInvocationFuture invocationFuture = new ClientInvocation(getClient(), request).invoke();
            return new ClientDelegatingFuture<Long>(
                    invocationFuture,
                    getContext().getSerializationService(),
                    ADD_ASYNC_ASYNC_RESPONSE_DECODER);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    @Override
    public E readOne(long sequence) throws InterruptedException {
        checkSequence(sequence);

        ClientMessage request = RingbufferReadOneCodec.encodeRequest(name, sequence);
        ClientMessage response = invoke(request, partitionId);
        RingbufferReadOneCodec.ResponseParameters resultParameters = RingbufferReadOneCodec.decodeResponse(response);
        return toObject(resultParameters.response);
    }

    @Override
    public ICompletableFuture<Long> addAllAsync(Collection<? extends E> collection, OverflowPolicy overflowPolicy) {
        checkNotNull(collection, "collection can't be null");
        checkNotNull(overflowPolicy, "overflowPolicy can't be null");
        checkFalse(collection.isEmpty(), "collection can't be empty");
        checkTrue(collection.size() <= MAX_BATCH_SIZE, "collection can't be larger than " + MAX_BATCH_SIZE);

        final List<Data> valueList = new ArrayList<Data>(collection.size());
        for (E e : collection) {
            throwExceptionIfNull(e);
            valueList.add(toData(e));
        }

        ClientMessage request = RingbufferAddAllAsyncCodec.encodeRequest(name, valueList, overflowPolicy.getId());
        request.setPartitionId(partitionId);

        try {
            ClientInvocationFuture invocationFuture = new ClientInvocation(getClient(), request).invoke();
            return new ClientDelegatingFuture<Long>(
                    invocationFuture,
                    getContext().getSerializationService(),
                    ADD_ALL_ASYNC_RESPONSE_DECODER);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    @Override
    public ICompletableFuture<ReadResultSet<E>> readManyAsync(long startSequence, int minCount,
                                                              int maxCount, IFunction<E, Boolean> filter) {

        checkSequence(startSequence);
        checkNotNegative(minCount, "minCount can't be smaller than 0");
        checkTrue(maxCount >= minCount, "maxCount should be equal or larger than minCount");
        checkTrue(minCount <= capacity(), "the minCount should be smaller than or equal to the capacity");
        checkTrue(maxCount <= MAX_BATCH_SIZE, "maxCount can't be larger than " + MAX_BATCH_SIZE);

        ClientMessage request = RingbufferReadManyAsyncCodec.encodeRequest(
                name,
                startSequence,
                minCount,
                maxCount,
                toData(filter));
        request.setPartitionId(partitionId);

        try {
            ClientInvocationFuture invocationFuture = new ClientInvocation(getClient(), request).invoke();
            return new ClientDelegatingFuture<ReadResultSet<E>>(
                    invocationFuture,
                    getContext().getSerializationService(),
                    readManyAsyncResponseDecoder);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    private static void checkSequence(long sequence) {
        if (sequence < 0) {
            throw new IllegalArgumentException("sequence can't be smaller than 0, but was: " + sequence);
        }
    }

    protected <T> T invoke(ClientMessage clientMessage, int partitionId) {
        try {
            final Future future = new ClientInvocation(getClient(), clientMessage, partitionId).invoke();
            return (T) future.get();
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }
}
