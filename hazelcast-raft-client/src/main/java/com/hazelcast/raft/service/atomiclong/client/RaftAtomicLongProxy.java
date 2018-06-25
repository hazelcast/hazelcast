/*
 *  Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.raft.service.atomiclong.client;

import com.hazelcast.client.impl.ClientMessageDecoder;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.client.spi.impl.ClientInvocationFuture;
import com.hazelcast.client.util.ClientDelegatingFuture;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IFunction;
import com.hazelcast.nio.Bits;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.RaftGroupIdImpl;
import com.hazelcast.raft.service.atomiclong.RaftAtomicLongService;
import com.hazelcast.spi.InternalCompletableFuture;

import static com.hazelcast.client.impl.protocol.util.ParameterUtil.calculateDataSize;
import static com.hazelcast.raft.impl.service.RaftService.getObjectNameForProxy;
import static com.hazelcast.raft.service.atomiclong.client.AtomicLongMessageTaskFactoryProvider.ADD_AND_GET_TYPE;
import static com.hazelcast.raft.service.atomiclong.client.AtomicLongMessageTaskFactoryProvider.COMPARE_AND_SET_TYPE;
import static com.hazelcast.raft.service.atomiclong.client.AtomicLongMessageTaskFactoryProvider.DESTROY_TYPE;
import static com.hazelcast.raft.service.atomiclong.client.AtomicLongMessageTaskFactoryProvider.GET_AND_ADD_TYPE;
import static com.hazelcast.raft.service.atomiclong.client.AtomicLongMessageTaskFactoryProvider.GET_AND_SET_TYPE;
import static com.hazelcast.raft.service.spi.client.RaftGroupTaskFactoryProvider.CREATE_TYPE;
import static com.hazelcast.raft.service.util.ClientAccessor.getClient;

/**
 * TODO: Javadoc Pending...
 */
public class RaftAtomicLongProxy implements IAtomicLong {

    private static final ClientMessageDecoder LONG_RESPONSE_DECODER = new LongResponseDecoder();
    private static final ClientMessageDecoder BOOLEAN_RESPONSE_DECODER = new BooleanResponseDecoder();

    public static IAtomicLong create(HazelcastInstance instance, String name) {
        int dataSize = ClientMessage.HEADER_SIZE + calculateDataSize(name);
        ClientMessage msg = ClientMessage.createForEncode(dataSize);
        msg.setMessageType(CREATE_TYPE);
        msg.setRetryable(false);
        msg.setOperationName("");
        msg.set(name);
        msg.updateFrameLength();

        String objectName = getObjectNameForProxy(name);
        HazelcastClientInstanceImpl client = getClient(instance);
        ClientInvocationFuture f = new ClientInvocation(client, msg, objectName).invoke();

        InternalCompletableFuture<RaftGroupId> future = new ClientDelegatingFuture<RaftGroupId>(f, client.getSerializationService(),
                new ClientMessageDecoder() {
            @Override
            public RaftGroupId decodeClientMessage(ClientMessage msg) {
                return RaftGroupIdImpl.readFrom(msg);
            }
        });
        RaftGroupId groupId = future.join();
        return new RaftAtomicLongProxy(instance, groupId, objectName);
    }

    private final HazelcastClientInstanceImpl client;
    private final RaftGroupId groupId;
    private final String name;

    private RaftAtomicLongProxy(HazelcastInstance instance, RaftGroupId groupId, String name) {
        client = getClient(instance);
        this.groupId = groupId;
        this.name = name;
    }

    @Override
    public long addAndGet(long delta) {
        return addAndGetAsync(delta).join();
    }

    @Override
    public boolean compareAndSet(long expect, long update) {
        return compareAndSetAsync(expect, update).join();
    }

    @Override
    public long decrementAndGet() {
        return decrementAndGetAsync().join();
    }

    @Override
    public long get() {
        return getAsync().join();
    }

    @Override
    public long getAndAdd(long delta) {
        return getAndAddAsync(delta).join();
    }

    @Override
    public long getAndSet(long newValue) {
        return getAndSetAsync(newValue).join();
    }

    @Override
    public long incrementAndGet() {
        return incrementAndGetAsync().join();
    }

    @Override
    public long getAndIncrement() {
        return getAndIncrementAsync().join();
    }

    @Override
    public void set(long newValue) {
        setAsync(newValue).join();
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
    public <R> R apply(IFunction<Long, R> function) {
        return applyAsync(function).join();
    }

    @Override
    public InternalCompletableFuture<Long> addAndGetAsync(long delta) {
        ClientMessage msg = encodeRequest(groupId, name, delta, ADD_AND_GET_TYPE);
        return invoke(msg, LONG_RESPONSE_DECODER);
    }

    @Override
    public InternalCompletableFuture<Boolean> compareAndSetAsync(long expect, long update) {
        ClientMessage msg = encodeRequest(groupId, name, expect, update, COMPARE_AND_SET_TYPE);
        return invoke(msg, BOOLEAN_RESPONSE_DECODER);
    }

    @Override
    public InternalCompletableFuture<Long> decrementAndGetAsync() {
        return addAndGetAsync(-1);
    }

    @Override
    public InternalCompletableFuture<Long> getAsync() {
        return getAndAddAsync(0);
    }

    @Override
    public InternalCompletableFuture<Long> getAndAddAsync(long delta) {
        ClientMessage msg = encodeRequest(groupId, name, delta, GET_AND_ADD_TYPE);
        return invoke(msg, LONG_RESPONSE_DECODER);
    }

    @Override
    public InternalCompletableFuture<Long> getAndSetAsync(long newValue) {
        ClientMessage msg = encodeRequest(groupId, name, newValue, GET_AND_SET_TYPE);
        return invoke(msg, LONG_RESPONSE_DECODER);
    }

    @Override
    public InternalCompletableFuture<Long> incrementAndGetAsync() {
        return addAndGetAsync(1);
    }

    @Override
    public InternalCompletableFuture<Long> getAndIncrementAsync() {
        return getAndAddAsync(1);
    }

    @Override
    public InternalCompletableFuture<Void> setAsync(long newValue) {
        InternalCompletableFuture future = getAndSetAsync(newValue);
        return future;
    }

    @Override
    public InternalCompletableFuture<Void> alterAsync(IFunction<Long, Long> function) {
        throw new UnsupportedOperationException();
    }

    @Override
    public InternalCompletableFuture<Long> alterAndGetAsync(IFunction<Long, Long> function) {
        throw new UnsupportedOperationException();
    }

    @Override
    public InternalCompletableFuture<Long> getAndAlterAsync(IFunction<Long, Long> function) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <R> InternalCompletableFuture<R> applyAsync(IFunction<Long, R> function) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getServiceName() {
        return RaftAtomicLongService.SERVICE_NAME;
    }

    @Override
    public String getPartitionKey() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void destroy() {
        int dataSize = ClientMessage.HEADER_SIZE + RaftGroupIdImpl.dataSize(groupId) + calculateDataSize(name);
        ClientMessage msg = prepareClientMessage(groupId, name, dataSize, DESTROY_TYPE);
        msg.updateFrameLength();

        invoke(msg, BOOLEAN_RESPONSE_DECODER).join();
    }

    private <T> InternalCompletableFuture<T> invoke(ClientMessage msg, ClientMessageDecoder decoder) {
        ClientInvocationFuture future = new ClientInvocation(client, msg, getName()).invoke();
        return new ClientDelegatingFuture<T>(future, client.getSerializationService(), decoder);
    }

    private static ClientMessage encodeRequest(RaftGroupId groupId, String name, long value, int messageTypeId) {
        int dataSize = ClientMessage.HEADER_SIZE
                + RaftGroupIdImpl.dataSize(groupId) + calculateDataSize(name) + Bits.LONG_SIZE_IN_BYTES;
        ClientMessage msg = prepareClientMessage(groupId, name, dataSize, messageTypeId);
        msg.set(value);
        msg.updateFrameLength();
        return msg;
    }

    private static ClientMessage encodeRequest(RaftGroupId groupId, String name, long value1, long value2, int messageTypeId) {
        int dataSize = ClientMessage.HEADER_SIZE
                + RaftGroupIdImpl.dataSize(groupId) + calculateDataSize(name) + 2 * Bits.LONG_SIZE_IN_BYTES;
        ClientMessage msg = prepareClientMessage(groupId, name, dataSize, messageTypeId);
        msg.set(value1);
        msg.set(value2);
        msg.updateFrameLength();
        return msg;
    }

    private static ClientMessage prepareClientMessage(RaftGroupId groupId, String name, int dataSize, int messageTypeId) {
        ClientMessage msg = ClientMessage.createForEncode(dataSize);
        msg.setMessageType(messageTypeId);
        msg.setRetryable(false);
        msg.setOperationName("");
        RaftGroupIdImpl.writeTo(groupId, msg);
        msg.set(name);
        return msg;
    }

    public RaftGroupId getGroupId() {
        return groupId;
    }

    private static class LongResponseDecoder implements ClientMessageDecoder {
        @Override
        public Long decodeClientMessage(ClientMessage msg) {
            return msg.getLong();
        }
    }

    private static class BooleanResponseDecoder implements ClientMessageDecoder {
        @Override
        public Boolean decodeClientMessage(ClientMessage msg) {
            return msg.getBoolean();
        }
    }
}
