package com.hazelcast.raft.service.lock.client;

import com.hazelcast.client.impl.ClientMessageDecoder;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.HazelcastClientProxy;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.client.spi.impl.ClientInvocationFuture;
import com.hazelcast.client.util.ClientDelegatingFuture;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.ICondition;
import com.hazelcast.core.ILock;
import com.hazelcast.nio.Bits;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.RaftGroupIdImpl;
import com.hazelcast.raft.service.lock.RaftLockService;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.ThreadUtil;
import com.hazelcast.util.UuidUtil;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

import static com.hazelcast.client.impl.protocol.util.ParameterUtil.calculateDataSize;
import static com.hazelcast.raft.service.lock.client.LockMessageTaskFactoryProvider.CREATE_TYPE;
import static com.hazelcast.raft.service.lock.client.LockMessageTaskFactoryProvider.LOCK;
import static com.hazelcast.raft.service.lock.client.LockMessageTaskFactoryProvider.LOCK_COUNT;
import static com.hazelcast.raft.service.lock.client.LockMessageTaskFactoryProvider.TRY_LOCK;
import static com.hazelcast.raft.service.lock.client.LockMessageTaskFactoryProvider.UNLOCK;

/**
 * TODO: Javadoc Pending...
 *
 */
public class RaftLockProxy implements ILock {

    private static final ClientMessageDecoder INT_RESPONSE_DECODER = new IntResponseDecoder();
    private static final ClientMessageDecoder BOOLEAN_RESPONSE_DECODER = new BooleanResponseDecoder();

    public static ILock create(HazelcastInstance instance, String name) {
        int dataSize = ClientMessage.HEADER_SIZE + calculateDataSize(name);
        ClientMessage clientMessage = ClientMessage.createForEncode(dataSize);
        clientMessage.setMessageType(CREATE_TYPE);
        clientMessage.setRetryable(false);
        clientMessage.setOperationName("");
        clientMessage.set(name);
        clientMessage.updateFrameLength();

        HazelcastClientInstanceImpl client = getClient(instance);
        ClientInvocationFuture f = new ClientInvocation(client, clientMessage, name).invoke();

        ICompletableFuture<RaftGroupId> future = new ClientDelegatingFuture<RaftGroupId>(f, client.getSerializationService(),
                new ClientMessageDecoder() {
            @Override
            public RaftGroupId decodeClientMessage(ClientMessage clientMessage) {
                return RaftGroupIdImpl.readFrom(clientMessage);
            }
        });

        RaftGroupId groupId = join(future);
        return new RaftLockProxy(instance, groupId, name);
    }

    private final HazelcastClientInstanceImpl client;
    private final RaftGroupId groupId;
    private final String name;

    private RaftLockProxy(HazelcastInstance instance, RaftGroupId groupId, String name) {
        client = getClient(instance);
        this.groupId = groupId;
        this.name = name;
    }

    private static HazelcastClientInstanceImpl getClient(HazelcastInstance instance) {
        if (instance instanceof HazelcastClientProxy) {
            return  ((HazelcastClientProxy) instance).client;
        } else if (instance instanceof HazelcastClientInstanceImpl) {
            return  (HazelcastClientInstanceImpl) instance;
        } else {
            throw new IllegalArgumentException("Unknown client instance! " + instance);
        }
    }

    @Override
    public void lock() {
        String uuid = client.getLocalEndpoint().getUuid();
        UUID invUid = UuidUtil.newUnsecureUUID();
        ClientMessage clientMessage = encodeRequest(groupId, name, uuid, ThreadUtil.getThreadId(), invUid, LOCK);
        join(invoke(clientMessage, BOOLEAN_RESPONSE_DECODER));
    }

    @Override
    public void unlock() {
        String uuid = client.getLocalEndpoint().getUuid();
        UUID invUid = UuidUtil.newUnsecureUUID();
        ClientMessage clientMessage = encodeRequest(groupId, name, uuid, ThreadUtil.getThreadId(), invUid, UNLOCK);
        join(invoke(clientMessage, BOOLEAN_RESPONSE_DECODER));

    }

    @Override
    public boolean isLocked() {
        return getLockCount() > 0;
    }

    @Override
    public boolean isLockedByCurrentThread() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getLockCount() {
        String uuid = client.getLocalEndpoint().getUuid();
        ClientMessage clientMessage = encodeRequest(groupId, name, uuid, LOCK_COUNT);
        ICompletableFuture<Integer> future = invoke(clientMessage, INT_RESPONSE_DECODER);
        return join(future);
    }

    @Override
    public boolean tryLock() {
        String uuid = client.getLocalEndpoint().getUuid();
        UUID invUid = UuidUtil.newUnsecureUUID();
        ClientMessage clientMessage = encodeRequest(groupId, name, uuid, ThreadUtil.getThreadId(), invUid, TRY_LOCK);
        ICompletableFuture<Boolean> future = invoke(clientMessage, BOOLEAN_RESPONSE_DECODER);
        return join(future);
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit, long leaseTime, TimeUnit leaseUnit) throws InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void lock(long leaseTime, TimeUnit timeUnit) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void forceUnlock() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Condition newCondition() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ICondition newCondition(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getRemainingLeaseTime() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getServiceName() {
        return RaftLockService.SERVICE_NAME;
    }

    @Override
    public String getPartitionKey() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getKey() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void destroy() {
        throw new UnsupportedOperationException();
    }

    private <T> ICompletableFuture<T> invoke(ClientMessage clientMessage, ClientMessageDecoder decoder) {
        ClientInvocationFuture future = new ClientInvocation(client, clientMessage, getName()).invoke();
        return new ClientDelegatingFuture<T>(future, client.getSerializationService(), decoder);
    }

    private static <T> T join(ICompletableFuture<T> future) {
        try {
            return future.get();
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    private static ClientMessage encodeRequest(RaftGroupId groupId, String name, String uuid, long threadId, UUID invUid, int messageTypeId) {
        int dataSize = ClientMessage.HEADER_SIZE
                + RaftGroupIdImpl.dataSize(groupId) + calculateDataSize(name) + calculateDataSize(uuid) + Bits.LONG_SIZE_IN_BYTES * 3;
        ClientMessage clientMessage = prepareClientMessage(groupId, name, dataSize, messageTypeId);
        clientMessage.set(uuid);
        clientMessage.set(threadId);
        clientMessage.set(invUid.getLeastSignificantBits());
        clientMessage.set(invUid.getMostSignificantBits());
        clientMessage.updateFrameLength();
        return clientMessage;
    }

    private static ClientMessage encodeRequest(RaftGroupId groupId, String name, String uuid, int messageTypeId) {
        int dataSize = ClientMessage.HEADER_SIZE
                + RaftGroupIdImpl.dataSize(groupId) + calculateDataSize(name) + calculateDataSize(uuid);
        ClientMessage clientMessage = prepareClientMessage(groupId, name, dataSize, messageTypeId);
        clientMessage.set(uuid);
        clientMessage.updateFrameLength();
        return clientMessage;
    }


    private static ClientMessage prepareClientMessage(RaftGroupId groupId, String name, int dataSize, int messageTypeId) {
        ClientMessage clientMessage = ClientMessage.createForEncode(dataSize);
        clientMessage.setMessageType(messageTypeId);
        clientMessage.setRetryable(false);
        clientMessage.setOperationName("");
        RaftGroupIdImpl.writeTo(groupId, clientMessage);
        clientMessage.set(name);
        return clientMessage;
    }

    private static class IntResponseDecoder implements ClientMessageDecoder {
        @Override
        public Integer decodeClientMessage(ClientMessage clientMessage) {
            return clientMessage.getInt();
        }
    }

    private static class BooleanResponseDecoder implements ClientMessageDecoder {
        @Override
        public Boolean decodeClientMessage(ClientMessage clientMessage) {
            return clientMessage.getBoolean();
        }
    }
}
