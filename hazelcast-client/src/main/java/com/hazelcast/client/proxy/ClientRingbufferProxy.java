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

import com.hazelcast.client.impl.ClientMessageDecoder;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.RingbufferAddAllCodec;
import com.hazelcast.client.impl.protocol.codec.RingbufferAddCodec;
import com.hazelcast.client.impl.protocol.codec.RingbufferCapacityCodec;
import com.hazelcast.client.impl.protocol.codec.RingbufferHeadSequenceCodec;
import com.hazelcast.client.impl.protocol.codec.RingbufferReadManyCodec;
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
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.strategy.StringPartitioningStrategy;
import com.hazelcast.ringbuffer.OverflowPolicy;
import com.hazelcast.ringbuffer.ReadResultSet;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.ringbuffer.StaleSequenceException;
import com.hazelcast.ringbuffer.impl.client.PortableReadResultSet;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.CollectionUtil;
import com.hazelcast.util.ExceptionUtil;

import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.hazelcast.ringbuffer.impl.RingbufferProxy.MAX_BATCH_SIZE;
import static com.hazelcast.util.Preconditions.checkFalse;
import static com.hazelcast.util.Preconditions.checkNotNegative;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static com.hazelcast.util.Preconditions.checkTrue;
import static java.lang.String.format;

/**
 * Proxy implementation of {@link Ringbuffer}.
 *
 * @param <E> the type of elements in this ringbuffer
 */
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
            return RingbufferAddAllCodec.decodeResponse(clientMessage).response;
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
        String partitionKey = StringPartitioningStrategy.getPartitionKey(name);
        partitionId = getContext().getPartitionService().getPartitionId(partitionKey);
        final SerializationService serializationService = getContext().getSerializationService();

        readManyAsyncResponseDecoder = new ClientMessageDecoder() {
            @Override
            public PortableReadResultSet decodeClientMessage(ClientMessage clientMessage) {
                final RingbufferReadManyCodec.ResponseParameters responseParameters
                        = RingbufferReadManyCodec.decodeResponse(clientMessage);
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
        ClientMessage request = RingbufferAddCodec.encodeRequest(name, overflowPolicy.getId(), element);

        try {
            ClientInvocationFuture invocationFuture = new ClientInvocation(getClient(), request, partitionId).invoke();
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

        Collection<Data> dataCollection = CollectionUtil.objectToDataCollection(collection, getSerializationService());
        ClientMessage request = RingbufferAddAllCodec.encodeRequest(name, dataCollection, overflowPolicy.getId());

        try {
            ClientInvocationFuture invocationFuture = new ClientInvocation(getClient(), request, partitionId).invoke();
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

        ClientMessage request = RingbufferReadManyCodec.encodeRequest(
                name,
                startSequence,
                minCount,
                maxCount,
                toData(filter));

        try {
            ClientInvocationFuture invocationFuture = new ClientInvocation(getClient(), request, partitionId).invoke();
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
        } catch (ExecutionException e) {
            if (e.getCause() instanceof StaleSequenceException) {
                StaleSequenceException se = (StaleSequenceException) e.getCause();
                long l = headSequence();
                throw new StaleSequenceException(se.getMessage(), l);
            }
            throw ExceptionUtil.rethrow(e);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    @Override
    public String toString() {
        return format("Ringbuffer{name='%s'}", name);
    }
}
