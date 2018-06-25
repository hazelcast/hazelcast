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

package com.hazelcast.raft.service.semaphore.client;

import com.hazelcast.client.impl.ClientMessageDecoder;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.client.spi.impl.ClientInvocationFuture;
import com.hazelcast.client.util.ClientDelegatingFuture;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ISemaphore;
import com.hazelcast.nio.Bits;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.RaftGroupIdImpl;
import com.hazelcast.raft.service.semaphore.RaftSemaphoreService;
import com.hazelcast.raft.service.spi.client.RaftGroupTaskFactoryProvider;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.util.ConstructorFunction;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.client.impl.protocol.util.ParameterUtil.calculateDataSize;
import static com.hazelcast.raft.impl.RaftGroupIdImpl.dataSize;
import static com.hazelcast.raft.impl.service.RaftService.getObjectNameForProxy;
import static com.hazelcast.raft.service.atomiclong.client.AtomicLongMessageTaskFactoryProvider.ADD_AND_GET_TYPE;
import static com.hazelcast.raft.service.semaphore.client.SemaphoreMessageTaskFactoryProvider.ACQUIRE_PERMITS_TYPE;
import static com.hazelcast.raft.service.semaphore.client.SemaphoreMessageTaskFactoryProvider.AVAILABLE_PERMITS_TYPE;
import static com.hazelcast.raft.service.semaphore.client.SemaphoreMessageTaskFactoryProvider.CHANGE_PERMITS_TYPE;
import static com.hazelcast.raft.service.semaphore.client.SemaphoreMessageTaskFactoryProvider.DESTROY_TYPE;
import static com.hazelcast.raft.service.semaphore.client.SemaphoreMessageTaskFactoryProvider.DRAIN_PERMITS_TYPE;
import static com.hazelcast.raft.service.semaphore.client.SemaphoreMessageTaskFactoryProvider.INIT_SEMAPHORE_TYPE;
import static com.hazelcast.raft.service.semaphore.client.SemaphoreMessageTaskFactoryProvider.RELEASE_PERMITS_TYPE;
import static com.hazelcast.raft.service.semaphore.proxy.GloballyUniqueThreadIdUtil.getGlobalThreadId;
import static com.hazelcast.raft.service.session.AbstractSessionManager.NO_SESSION_ID;
import static com.hazelcast.raft.service.util.ClientAccessor.getClient;
import static com.hazelcast.util.Preconditions.checkNotNegative;
import static com.hazelcast.util.Preconditions.checkPositive;
import static com.hazelcast.util.UuidUtil.newUnsecureUUID;
import static java.lang.Math.max;

/**
 * TODO: Javadoc Pending...
 */
public class RaftSessionlessSemaphoreProxy implements ISemaphore {

    private static final ClientMessageDecoder INT_RESPONSE_DECODER = new IntResponseDecoder();
    private static final ClientMessageDecoder LONG_RESPONSE_DECODER = new LongResponseDecoder();
    private static final ClientMessageDecoder BOOLEAN_RESPONSE_DECODER = new BooleanResponseDecoder();

    public static ISemaphore create(HazelcastInstance instance, String name) {
        int dataSize = ClientMessage.HEADER_SIZE + calculateDataSize(name);
        ClientMessage msg = ClientMessage.createForEncode(dataSize);
        msg.setMessageType(RaftGroupTaskFactoryProvider.CREATE_TYPE);
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
        return new RaftSessionlessSemaphoreProxy(instance, groupId, objectName);
    }

    private final HazelcastClientInstanceImpl client;
    private final RaftGroupId groupId;
    private final String name;
    private final ConstructorFunction<RaftGroupId, Long> globallyUniqueThreadIdCtor;

    private RaftSessionlessSemaphoreProxy(HazelcastInstance instance, RaftGroupId groupId, final String name) {
        this.client = getClient(instance);
        this.groupId = groupId;
        this.name = name;
        this.globallyUniqueThreadIdCtor = new ConstructorFunction<RaftGroupId, Long>() {
            @Override
            public Long createNew(RaftGroupId groupId) {
                int dataSize = ClientMessage.HEADER_SIZE
                        + RaftGroupIdImpl.dataSize(groupId) + calculateDataSize(name)
                        + Bits.LONG_SIZE_IN_BYTES;

                ClientMessage msg = ClientMessage.createForEncode(dataSize);
                msg.setMessageType(ADD_AND_GET_TYPE);
                msg.setRetryable(false);
                msg.setOperationName("");
                RaftGroupIdImpl.writeTo(groupId, msg);
                msg.set(name);
                msg.set(1L);
                msg.updateFrameLength();

                ClientInvocationFuture future = new ClientInvocation(client, msg, getName()).invoke();
                return new ClientDelegatingFuture<Long>(future, client.getSerializationService(), LONG_RESPONSE_DECODER).join();
            }
        };
    }

    @Override
    public boolean init(int permits) {
        checkNotNegative(permits, "Permits must be non-negative!");

        int dataSize = ClientMessage.HEADER_SIZE + dataSize(groupId) + calculateDataSize(name) + Bits.LONG_SIZE_IN_BYTES
                + Bits.INT_SIZE_IN_BYTES;
        ClientMessage msg = prepareClientMessage(groupId, name, dataSize, INIT_SEMAPHORE_TYPE);
        msg.set(permits);
        msg.updateFrameLength();

        InternalCompletableFuture<Boolean> future = invoke(msg, BOOLEAN_RESPONSE_DECODER);
        return future.join();
    }

    @Override
    public void acquire() {
        acquire(1);
    }

    @Override
    public void acquire(int permits) {
        checkPositive(permits, "Permits must be positive!");

        long globalThreadId = getGlobalThreadId(groupId, globallyUniqueThreadIdCtor);
        UUID invocationUid = newUnsecureUUID();
        int dataSize = ClientMessage.HEADER_SIZE + dataSize(groupId) + calculateDataSize(name) + Bits.LONG_SIZE_IN_BYTES * 5
                + Bits.INT_SIZE_IN_BYTES;
        ClientMessage msg = prepareClientMessage(groupId, name, dataSize, ACQUIRE_PERMITS_TYPE);
        msg.set(globalThreadId);
        msg.set(invocationUid.getLeastSignificantBits());
        msg.set(invocationUid.getMostSignificantBits());
        msg.set(permits);
        msg.set(-1L);
        msg.updateFrameLength();

        invoke(msg, BOOLEAN_RESPONSE_DECODER).join();
    }

    @Override
    public boolean tryAcquire() {
        return tryAcquire(1);
    }

    @Override
    public boolean tryAcquire(int permits) {
        return tryAcquire(permits, 0, TimeUnit.MILLISECONDS);
    }

    @Override
    public boolean tryAcquire(long timeout, TimeUnit unit) {
        return tryAcquire(1, timeout, unit);
    }

    @Override
    public boolean tryAcquire(int permits, long timeout, TimeUnit unit) {
        checkPositive(permits, "Permits must be positive!");

        long globalThreadId = getGlobalThreadId(groupId, globallyUniqueThreadIdCtor);
        UUID invocationUid = newUnsecureUUID();
        long timeoutMs = max(0, unit.toMillis(timeout));
        int dataSize = ClientMessage.HEADER_SIZE + dataSize(groupId) + calculateDataSize(name) + Bits.LONG_SIZE_IN_BYTES * 5
                + Bits.INT_SIZE_IN_BYTES;
        ClientMessage msg = prepareClientMessage(groupId, name, dataSize, ACQUIRE_PERMITS_TYPE);
        msg.set(globalThreadId);
        msg.set(invocationUid.getLeastSignificantBits());
        msg.set(invocationUid.getMostSignificantBits());
        msg.set(permits);
        msg.set(timeoutMs);
        msg.updateFrameLength();

        InternalCompletableFuture<Boolean> future = invoke(msg, BOOLEAN_RESPONSE_DECODER);
        return future.join();
    }

    @Override
    public void release() {
        release(1);
    }

    @Override
    public void release(int permits) {
        checkPositive(permits, "Permits must be positive!");

        long globalThreadId = getGlobalThreadId(groupId, globallyUniqueThreadIdCtor);
        UUID invocationUid = newUnsecureUUID();
        int dataSize = ClientMessage.HEADER_SIZE + dataSize(groupId) + calculateDataSize(name) + Bits.LONG_SIZE_IN_BYTES * 4
                + Bits.INT_SIZE_IN_BYTES;
        ClientMessage msg = prepareClientMessage(groupId, name, dataSize, RELEASE_PERMITS_TYPE);
        msg.set(globalThreadId);
        msg.set(invocationUid.getLeastSignificantBits());
        msg.set(invocationUid.getMostSignificantBits());
        msg.set(permits);
        msg.updateFrameLength();

        invoke(msg, BOOLEAN_RESPONSE_DECODER).join();
    }

    @Override
    public int availablePermits() {
        int dataSize = ClientMessage.HEADER_SIZE + dataSize(groupId) + calculateDataSize(name) + Bits.LONG_SIZE_IN_BYTES;
        ClientMessage msg = prepareClientMessage(groupId, name, dataSize, AVAILABLE_PERMITS_TYPE);
        msg.updateFrameLength();

        InternalCompletableFuture<Integer> future = invoke(msg, INT_RESPONSE_DECODER);
        return future.join();
    }

    @Override
    public int drainPermits() {
        long globalThreadId = getGlobalThreadId(groupId, globallyUniqueThreadIdCtor);
        UUID invocationUid = newUnsecureUUID();
        int dataSize = ClientMessage.HEADER_SIZE + dataSize(groupId) + calculateDataSize(name) + Bits.LONG_SIZE_IN_BYTES * 4;
        ClientMessage msg = prepareClientMessage(groupId, name, dataSize, DRAIN_PERMITS_TYPE);
        msg.set(globalThreadId);
        msg.set(invocationUid.getLeastSignificantBits());
        msg.set(invocationUid.getMostSignificantBits());
        msg.updateFrameLength();

        InternalCompletableFuture<Integer> future = invoke(msg, INT_RESPONSE_DECODER);
        return future.join();
    }

    @Override
    public void reducePermits(int reduction) {
        checkNotNegative(reduction, "Reduction must be non-negative!");
        if (reduction == 0) {
            return;
        }

        long globalThreadId = getGlobalThreadId(groupId, globallyUniqueThreadIdCtor);
        UUID invocationUid = newUnsecureUUID();

        int dataSize = ClientMessage.HEADER_SIZE + dataSize(groupId) + calculateDataSize(name) + Bits.LONG_SIZE_IN_BYTES * 4
                + Bits.INT_SIZE_IN_BYTES;
        ClientMessage msg = prepareClientMessage(groupId, name, dataSize, CHANGE_PERMITS_TYPE);
        msg.set(globalThreadId);
        msg.set(invocationUid.getLeastSignificantBits());
        msg.set(invocationUid.getMostSignificantBits());
        msg.set(-reduction);
        msg.updateFrameLength();

        invoke(msg, BOOLEAN_RESPONSE_DECODER).join();
    }

    @Override
    public void increasePermits(int increase) {
        checkNotNegative(increase, "Increase must be non-negative!");
        if (increase == 0) {
            return;
        }

        long globalThreadId = getGlobalThreadId(groupId, globallyUniqueThreadIdCtor);
        UUID invocationUid = newUnsecureUUID();

        int dataSize = ClientMessage.HEADER_SIZE + dataSize(groupId) + calculateDataSize(name) + Bits.LONG_SIZE_IN_BYTES * 4
                + Bits.INT_SIZE_IN_BYTES;
        ClientMessage msg = prepareClientMessage(groupId, name, dataSize, CHANGE_PERMITS_TYPE);
        msg.set(globalThreadId);
        msg.set(invocationUid.getLeastSignificantBits());
        msg.set(invocationUid.getMostSignificantBits());
        msg.set(increase);
        msg.updateFrameLength();

        invoke(msg, BOOLEAN_RESPONSE_DECODER).join();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getPartitionKey() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getServiceName() {
        return RaftSemaphoreService.SERVICE_NAME;
    }

    @Override
    public void destroy() {
        int dataSize = ClientMessage.HEADER_SIZE + RaftGroupIdImpl.dataSize(groupId) + calculateDataSize(name);
        ClientMessage msg = ClientMessage.createForEncode(dataSize);
        msg.setMessageType(DESTROY_TYPE);
        msg.setRetryable(false);
        msg.setOperationName("");
        RaftGroupIdImpl.writeTo(groupId, msg);
        msg.set(name);
        msg.updateFrameLength();

        invoke(msg, BOOLEAN_RESPONSE_DECODER).join();
    }

    public final RaftGroupId getGroupId() {
        return groupId;
    }

    private ClientMessage prepareClientMessage(RaftGroupId groupId, String name, int dataSize, int messageTypeId) {
        ClientMessage msg = ClientMessage.createForEncode(dataSize);
        msg.setMessageType(messageTypeId);
        msg.setRetryable(false);
        msg.setOperationName("");
        RaftGroupIdImpl.writeTo(groupId, msg);
        msg.set(name);
        msg.set(NO_SESSION_ID);
        return msg;
    }

    private <T> InternalCompletableFuture<T> invoke(ClientMessage msg, ClientMessageDecoder decoder) {
        ClientInvocationFuture future = new ClientInvocation(client, msg, name).invoke();
        return new ClientDelegatingFuture<T>(future, client.getSerializationService(), decoder);
    }

    private static class IntResponseDecoder implements ClientMessageDecoder {
        @Override
        public Integer decodeClientMessage(ClientMessage msg) {
            return msg.getInt();
        }
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
