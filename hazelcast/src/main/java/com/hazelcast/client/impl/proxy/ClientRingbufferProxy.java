/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.proxy;

import com.hazelcast.client.impl.ClientDelegatingFuture;
import com.hazelcast.client.impl.clientside.ClientMessageDecoder;
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
import com.hazelcast.client.impl.spi.ClientContext;
import com.hazelcast.client.impl.spi.ClientProxy;
import com.hazelcast.client.impl.spi.impl.ClientInvocation;
import com.hazelcast.client.impl.spi.impl.ClientInvocationFuture;
import com.hazelcast.core.IFunction;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.partition.strategy.StringPartitioningStrategy;
import com.hazelcast.ringbuffer.OverflowPolicy;
import com.hazelcast.ringbuffer.ReadResultSet;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.ringbuffer.StaleSequenceException;
import com.hazelcast.ringbuffer.impl.ReadResultSetImpl;
import com.hazelcast.spi.impl.InternalCompletableFuture;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.hazelcast.internal.util.CollectionUtil.objectToDataCollection;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.internal.util.Preconditions.checkFalse;
import static com.hazelcast.internal.util.Preconditions.checkNotNegative;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.Preconditions.checkTrue;
import static com.hazelcast.ringbuffer.impl.RingbufferProxy.MAX_BATCH_SIZE;
import static com.hazelcast.spi.impl.InternalCompletableFuture.completedExceptionally;
import static java.lang.String.format;

/**
 * Proxy implementation of {@link Ringbuffer}.
 *
 * @param <E> the type of elements in this ringbuffer
 */
public class ClientRingbufferProxy<E> extends ClientProxy implements Ringbuffer<E> {

    private static final ClientMessageDecoder ADD_ASYNC_ASYNC_RESPONSE_DECODER =
            clientMessage -> RingbufferAddCodec.decodeResponse(clientMessage).response;

    private static final ClientMessageDecoder ADD_ALL_ASYNC_RESPONSE_DECODER =
            clientMessage -> RingbufferAddAllCodec.decodeResponse(clientMessage).response;

    private ClientMessageDecoder readManyAsyncResponseDecoder;

    private int partitionId;
    private volatile long capacity = -1;

    public ClientRingbufferProxy(String serviceName, String objectName, ClientContext context) {
        super(serviceName, objectName, context);
    }

    @Override
    protected void onInitialize() {
        String partitionKey = StringPartitioningStrategy.getPartitionKey(name);
        partitionId = getContext().getPartitionService().getPartitionId(partitionKey);

        readManyAsyncResponseDecoder = clientMessage -> {
            final RingbufferReadManyCodec.ResponseParameters params = RingbufferReadManyCodec.decodeResponse(clientMessage);
            final ReadResultSetImpl readResultSet = new ReadResultSetImpl(
                    params.readCount, params.items, params.itemSeqs, params.nextSeq);
            readResultSet.setSerializationService(getSerializationService());
            return readResultSet;
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
    public long add(@Nonnull E item) {
        checkNotNull(item, "item can't be null");

        Data element = toData(item);
        ClientMessage request = RingbufferAddCodec.encodeRequest(name, OverflowPolicy.OVERWRITE.getId(), element);
        ClientMessage response = invoke(request, partitionId);
        RingbufferAddCodec.ResponseParameters resultParameters = RingbufferAddCodec.decodeResponse(response);
        return resultParameters.response;
    }

    @Override
    public InternalCompletableFuture<Long> addAsync(@Nonnull E item, @Nonnull OverflowPolicy overflowPolicy) {
        checkNotNull(item, "item can't be null");
        checkNotNull(overflowPolicy, "overflowPolicy can't be null");

        Data element = toData(item);
        ClientMessage request = RingbufferAddCodec.encodeRequest(name, overflowPolicy.getId(), element);
        try {
            ClientInvocationFuture invocationFuture = new ClientInvocation(getClient(), request, getName(), partitionId).invoke();
            return new ClientDelegatingFuture<>(invocationFuture, getSerializationService(),
                    ADD_ASYNC_ASYNC_RESPONSE_DECODER);
        } catch (Exception e) {
            throw rethrow(e);
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
    public InternalCompletableFuture<Long> addAllAsync(@Nonnull Collection<? extends E> collection,
                                                @Nonnull OverflowPolicy overflowPolicy) {
        checkNotNull(collection, "collection can't be null");
        checkNotNull(overflowPolicy, "overflowPolicy can't be null");
        checkFalse(collection.isEmpty(), "collection can't be empty");
        checkTrue(collection.size() <= MAX_BATCH_SIZE, "collection can't be larger than " + MAX_BATCH_SIZE);

        Collection<Data> dataCollection = objectToDataCollection(collection, getSerializationService());
        ClientMessage request = RingbufferAddAllCodec.encodeRequest(name, dataCollection, overflowPolicy.getId());

        try {
            ClientInvocationFuture invocationFuture = new ClientInvocation(getClient(), request, getName(), partitionId).invoke();
            return new ClientDelegatingFuture<>(invocationFuture, getSerializationService(), ADD_ALL_ASYNC_RESPONSE_DECODER);
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    @Override
    public InternalCompletableFuture<ReadResultSet<E>> readManyAsync(long startSequence, int minCount,
                                                              int maxCount, IFunction<E, Boolean> filter) {
        checkSequence(startSequence);
        checkNotNegative(minCount, "minCount can't be smaller than 0");
        checkTrue(maxCount >= minCount, "maxCount should be equal or larger than minCount");

        try {
            capacity();
        } catch (Throwable e) {
            return completedExceptionally(e);
        }

        checkTrue(maxCount <= capacity, "the maxCount should be smaller than or equal to the capacity");
        checkTrue(maxCount <= MAX_BATCH_SIZE, "maxCount can't be larger than " + MAX_BATCH_SIZE);

        ClientMessage request = RingbufferReadManyCodec.encodeRequest(
                name,
                startSequence,
                minCount,
                maxCount,
                toData(filter));

        try {
            ClientInvocationFuture invocationFuture = new ClientInvocation(getClient(), request, getName(), partitionId).invoke();
            return new ClientDelegatingFuture<>(invocationFuture, getSerializationService(),
                    readManyAsyncResponseDecoder);
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    private static void checkSequence(long sequence) {
        if (sequence < 0) {
            throw new IllegalArgumentException("sequence can't be smaller than 0, but was: " + sequence);
        }
    }

    protected <T> T invoke(ClientMessage clientMessage, int partitionId) {
        try {
            final Future future = new ClientInvocation(getClient(), clientMessage, getName(), partitionId).invoke();
            return (T) future.get();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof StaleSequenceException) {
                StaleSequenceException se = (StaleSequenceException) e.getCause();
                long l = headSequence();
                throw new StaleSequenceException(se.getMessage(), l);
            }
            throw rethrow(e);
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    @Override
    public String toString() {
        return format("Ringbuffer{name='%s'}", name);
    }
}
