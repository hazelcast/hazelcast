/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.connection.ClientBlockingChannel;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.AtomicLongAddAndGetCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicLongAlterAndGetCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicLongAlterCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicLongApplyCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicLongCompareAndSetCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicLongDecrementAndGetCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicLongGetAndAddCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicLongGetAndAlterCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicLongGetAndIncrementCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicLongGetAndSetCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicLongGetCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicLongIncrementAndGetCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicLongSetCodec;
import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.core.IFunction;
import com.hazelcast.cp.IAtomicLong;
import com.hazelcast.spi.InternalCompletableFuture;

import java.io.IOException;

import static com.hazelcast.util.Preconditions.isNotNull;

/**
 * Proxy implementation of {@link IAtomicLong}.
 */
@SuppressWarnings("checkstyle:methodcount")
public class ClientAtomicLongProxy extends PartitionSpecificClientProxy implements IAtomicLong {

    public ClientAtomicLongProxy(String serviceName, String objectId, ClientContext context) {
        super(serviceName, objectId, context);
    }

    @Override
    public <R> R apply(IFunction<Long, R> function) {
        return applyAsync(function).join();
    }

    @Override
    public void alter(IFunction<Long, Long> function) {
        alterAsync(function).join();
    }

    @Override
    public long alterAndGet(IFunction<Long, Long> function) {
        return alterAndGetAsync(function).join();
    }

    @Override
    public long getAndAlter(IFunction<Long, Long> function) {
        return getAndAlterAsync(function).join();
    }

    @Override
    public long addAndGet(long delta) {
        ClientMessage request = AtomicLongAddAndGetCodec.encodeRequest(name, delta);
        ClientMessage response = invokeOnPartition(request);
        return AtomicLongAddAndGetCodec.decodeResponse(response).response;
    }

    @Override
    public boolean compareAndSet(long expect, long update) {
        ClientMessage request = AtomicLongCompareAndSetCodec.encodeRequest(name, expect, update);
        ClientMessage response = invokeOnPartition(request);
        return AtomicLongCompareAndSetCodec.decodeResponse(response).response;
    }

    @Override
    public long decrementAndGet() {
        ClientMessage request = AtomicLongDecrementAndGetCodec.encodeRequest(name);
        ClientMessage response = invokeOnPartition(request);
        return AtomicLongDecrementAndGetCodec.decodeResponse(response).response;
    }

    @Override
    public long get() {
        ClientMessage request = AtomicLongGetCodec.encodeRequest(name);
        ClientMessage response = invokeOnPartition(request);
        return AtomicLongGetCodec.decodeResponse(response).response;
    }

    @Override
    public long getAndAdd(long delta) {
        ClientMessage request = AtomicLongGetAndAddCodec.encodeRequest(name, delta);
        ClientMessage response = invokeOnPartition(request);
        return AtomicLongGetAndAddCodec.decodeResponse(response).response;
    }

    @Override
    public long getAndSet(long newValue) {
        ClientMessage request = AtomicLongGetAndSetCodec.encodeRequest(name, newValue);
        ClientMessage response = invokeOnPartition(request);
        return AtomicLongGetAndSetCodec.decodeResponse(response).response;
    }

    public long pipelinedGet(int pipelineDepth) {
        ClientBlockingChannel channel = clientChannelPool.get(partitionId);

        Exception cause = null;
        try {
            for (int k = 0; k < pipelineDepth; k++) {
                ClientMessage request = AtomicLongGetCodec.encodeRequest(name);
                request.setPartitionId(partitionId);
                channel.write(request);
            }
            channel.flush();
            for (int k = 0; k < pipelineDepth; k++) {
                channel.readResponse();
            }
        } catch (IOException e) {
            cause = e;
            throw new RuntimeException(e);
        } finally {
            clientChannelPool.release(partitionId, channel, cause);
        }
        return 0;
    }

    @Override
    public long incrementAndGet() {
        ClientMessage request = AtomicLongIncrementAndGetCodec.encodeRequest(name);
        ClientMessage response = invokeOnPartition(request);
        return AtomicLongIncrementAndGetCodec.decodeResponse(response).response;
    }

    @Override
    public long getAndIncrement() {
        ClientMessage request = AtomicLongGetAndIncrementCodec.encodeRequest(name);
        ClientMessage response = invokeOnPartition(request);
        return AtomicLongGetAndIncrementCodec.decodeResponse(response).response;
    }

    @Override
    public void set(long newValue) {
        ClientMessage request = AtomicLongSetCodec.encodeRequest(name, newValue);
        invokeOnPartition(request);
    }

    @Override
    public InternalCompletableFuture<Long> addAndGetAsync(long delta) {
        ClientMessage request = AtomicLongAddAndGetCodec.encodeRequest(name, delta);
        return invokeOnPartitionAsync(request, message -> AtomicLongAddAndGetCodec.decodeResponse(message).response);
    }

    @Override
    public InternalCompletableFuture<Boolean> compareAndSetAsync(long expect, long update) {
        ClientMessage request = AtomicLongCompareAndSetCodec.encodeRequest(name, expect, update);
        return invokeOnPartitionAsync(request, message -> AtomicLongCompareAndSetCodec.decodeResponse(message).response);
    }

    @Override
    public InternalCompletableFuture<Long> decrementAndGetAsync() {
        ClientMessage request = AtomicLongDecrementAndGetCodec.encodeRequest(name);
        return invokeOnPartitionAsync(request, message -> AtomicLongDecrementAndGetCodec.decodeResponse(message).response);
    }

    @Override
    public InternalCompletableFuture<Long> getAsync() {
        ClientMessage request = AtomicLongGetCodec.encodeRequest(name);
        return invokeOnPartitionAsync(request, message -> AtomicLongGetCodec.decodeResponse(message).response);
    }

    @Override
    public InternalCompletableFuture<Long> getAndAddAsync(long delta) {
        ClientMessage request = AtomicLongGetAndAddCodec.encodeRequest(name, delta);
        return invokeOnPartitionAsync(request, message -> AtomicLongGetAndAddCodec.decodeResponse(message).response);
    }

    @Override
    public InternalCompletableFuture<Long> getAndSetAsync(long newValue) {
        ClientMessage request = AtomicLongGetAndSetCodec.encodeRequest(name, newValue);
        return invokeOnPartitionAsync(request, message -> AtomicLongGetAndSetCodec.decodeResponse(message).response);
    }

    @Override
    public InternalCompletableFuture<Long> incrementAndGetAsync() {
        ClientMessage request = AtomicLongIncrementAndGetCodec.encodeRequest(name);
        return invokeOnPartitionAsync(request, message -> AtomicLongIncrementAndGetCodec.decodeResponse(message).response);
    }

    @Override
    public InternalCompletableFuture<Long> getAndIncrementAsync() {
        ClientMessage request = AtomicLongGetAndIncrementCodec.encodeRequest(name);
        return invokeOnPartitionAsync(request, message -> AtomicLongGetAndIncrementCodec.decodeResponse(message).response);
    }

    @Override
    public InternalCompletableFuture<Void> setAsync(long newValue) {
        ClientMessage request = AtomicLongSetCodec.encodeRequest(name, newValue);
        return invokeOnPartitionAsync(request, clientMessage -> null);
    }

    @Override
    public InternalCompletableFuture<Void> alterAsync(IFunction<Long, Long> function) {
        isNotNull(function, "function");
        ClientMessage request = AtomicLongAlterCodec.encodeRequest(name, toData(function));
        return invokeOnPartitionAsync(request, clientMessage -> null);
    }

    @Override
    public InternalCompletableFuture<Long> alterAndGetAsync(IFunction<Long, Long> function) {
        isNotNull(function, "function");
        ClientMessage request = AtomicLongAlterAndGetCodec.encodeRequest(name, toData(function));
        return invokeOnPartitionAsync(request, message -> AtomicLongAlterAndGetCodec.decodeResponse(message).response);
    }

    @Override
    public InternalCompletableFuture<Long> getAndAlterAsync(IFunction<Long, Long> function) {
        isNotNull(function, "function");
        ClientMessage request = AtomicLongGetAndAlterCodec.encodeRequest(name, toData(function));
        return invokeOnPartitionAsync(request, message -> AtomicLongGetAndAlterCodec.decodeResponse(message).response);
    }

    @Override
    public <R> InternalCompletableFuture<R> applyAsync(IFunction<Long, R> function) {
        isNotNull(function, "function");
        ClientMessage request = AtomicLongApplyCodec.encodeRequest(name, toData(function));
        return invokeOnPartitionAsync(request, message -> AtomicLongApplyCodec.decodeResponse(message).response);
    }

    @Override
    public String toString() {
        return "IAtomicLong{" + "name='" + name + '\'' + '}';
    }
}
