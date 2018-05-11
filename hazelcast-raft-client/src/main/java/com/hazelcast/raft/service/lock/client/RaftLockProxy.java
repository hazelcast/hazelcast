package com.hazelcast.raft.service.lock.client;

import com.hazelcast.client.impl.ClientMessageDecoder;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
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
import com.hazelcast.raft.impl.session.SessionExpiredException;
import com.hazelcast.raft.service.lock.RaftLockService;
import com.hazelcast.raft.service.session.SessionAwareProxy;
import com.hazelcast.raft.service.session.SessionManagerProvider;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.ThreadUtil;
import com.hazelcast.util.UuidUtil;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

import static com.hazelcast.client.impl.protocol.util.ParameterUtil.calculateDataSize;
import static com.hazelcast.raft.service.lock.client.LockMessageTaskFactoryProvider.CREATE_TYPE;
import static com.hazelcast.raft.service.lock.client.LockMessageTaskFactoryProvider.DESTROY_TYPE;
import static com.hazelcast.raft.service.lock.client.LockMessageTaskFactoryProvider.LOCK;
import static com.hazelcast.raft.service.lock.client.LockMessageTaskFactoryProvider.LOCK_COUNT;
import static com.hazelcast.raft.service.lock.client.LockMessageTaskFactoryProvider.TRY_LOCK;
import static com.hazelcast.raft.service.lock.client.LockMessageTaskFactoryProvider.UNLOCK;
import static com.hazelcast.raft.service.util.ClientAccessor.getClient;

/**
 * TODO: Javadoc Pending...
 *
 */
public class RaftLockProxy extends SessionAwareProxy implements ILock {

    static final ClientMessageDecoder INT_RESPONSE_DECODER = new IntResponseDecoder();
    static final ClientMessageDecoder BOOLEAN_RESPONSE_DECODER = new BooleanResponseDecoder();
    static final ClientMessageDecoder LONG_RESPONSE_DECODER = new LongResponseDecoder();

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
        super(SessionManagerProvider.get(getClient(instance)), groupId);
        this.client = getClient(instance);
        this.groupId = groupId;
        this.name = name;
    }

    @Override
    public void lock() {
        UUID invUid = UuidUtil.newUnsecureUUID();
        for (;;) {
            long sessionId = acquireSession();
            ClientMessage clientMessage = encodeRequest(LOCK, groupId, name, sessionId, ThreadUtil.getThreadId(), invUid);
            ICompletableFuture<Object> future = invoke(client, LONG_RESPONSE_DECODER, name, clientMessage);
            try {
                join(future);
                break;
            } catch (SessionExpiredException e) {
                invalidateSession(sessionId);
            }
        }
    }

    @Override
    public boolean tryLock() {
        return tryLock(0, TimeUnit.MILLISECONDS);
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) {
        UUID invUid = UuidUtil.newUnsecureUUID();
        long timeoutMs = Math.max(0, unit.toMillis(time));
        for (;;) {
            long sessionId = acquireSession();
            ClientMessage clientMessage = encodeRequest(TRY_LOCK, groupId, name, sessionId, ThreadUtil.getThreadId(),
                    invUid, timeoutMs);
            ICompletableFuture<Long> future = invoke(client, LONG_RESPONSE_DECODER, name, clientMessage);
            try {
                return join(future) > 0L;
            } catch (SessionExpiredException e) {
                invalidateSession(sessionId);
            }
        }
    }

    @Override
    public void unlock() {
        final long sessionId = getSession();
        if (sessionId < 0) {
            throw new IllegalMonitorStateException();
        }
        UUID invUid = UuidUtil.newUnsecureUUID();
        ClientMessage clientMessage = encodeRequest(UNLOCK, groupId, name, sessionId, ThreadUtil.getThreadId(), invUid);
        ICompletableFuture<Object> future = invoke(client, BOOLEAN_RESPONSE_DECODER, name, clientMessage);
        try {
            join(future);
        } finally {
            releaseSession(sessionId);
        }
    }

    @Override
    public boolean isLocked() {
        return getLockCount() > 0;
    }

    @Override
    public boolean isLockedByCurrentThread() {
        long sessionId = getSession();
        if (sessionId < 0) {
            return false;
        }
        ClientMessage clientMessage = encodeRequest(LOCK_COUNT, groupId, name, sessionId, ThreadUtil.getThreadId());
        ICompletableFuture<Integer> future = invoke(client, INT_RESPONSE_DECODER, name, clientMessage);
        return join(future) > 0;
    }

    @Override
    public int getLockCount() {
        ClientMessage clientMessage = encodeRequest(LOCK_COUNT, groupId, name, -1, -1);
        ICompletableFuture<Integer> future = invoke(client, INT_RESPONSE_DECODER, name, clientMessage);
        return join(future);
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
        int dataSize = ClientMessage.HEADER_SIZE
                + RaftGroupIdImpl.dataSize(groupId) + calculateDataSize(name);
        ClientMessage clientMessage = prepareClientMessage(groupId, name, dataSize, DESTROY_TYPE);
        clientMessage.updateFrameLength();

        join(invoke(client, BOOLEAN_RESPONSE_DECODER, name, clientMessage));
    }

    static <T> ICompletableFuture<T> invoke(HazelcastClientInstanceImpl client, ClientMessageDecoder decoder, String name,
                                            ClientMessage clientMessage) {
        ClientInvocationFuture future = new ClientInvocation(client, clientMessage, name).invoke();
        return new ClientDelegatingFuture<T>(future, client.getSerializationService(), decoder);
    }

    static <T> T join(ICompletableFuture<T> future) {
        try {
            return future.get();
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    static ClientMessage encodeRequest(int messageTypeId, RaftGroupId groupId, String name, long sessionId,
            long threadId, UUID invUid) {
        int dataSize = ClientMessage.HEADER_SIZE
                + RaftGroupIdImpl.dataSize(groupId) + calculateDataSize(name) + Bits.LONG_SIZE_IN_BYTES * 4;
        ClientMessage clientMessage = prepareClientMessage(groupId, name, dataSize, messageTypeId);
        setRequestParams(clientMessage, sessionId, threadId, invUid);
        clientMessage.updateFrameLength();
        return clientMessage;
    }

    static ClientMessage encodeRequest(int messageTypeId, RaftGroupId groupId, String name, long sessionId,
            long threadId, UUID invUid, long val) {

        int dataSize = ClientMessage.HEADER_SIZE
                + RaftGroupIdImpl.dataSize(groupId) + calculateDataSize(name) + Bits.LONG_SIZE_IN_BYTES * 5;
        ClientMessage clientMessage = prepareClientMessage(groupId, name, dataSize, messageTypeId);
        setRequestParams(clientMessage, sessionId, threadId, invUid);
        clientMessage.set(val);
        clientMessage.updateFrameLength();
        return clientMessage;
    }

    private static void setRequestParams(ClientMessage clientMessage, long sessionId, long threadId, UUID invUid) {
        clientMessage.set(sessionId);
        clientMessage.set(threadId);
        clientMessage.set(invUid.getLeastSignificantBits());
        clientMessage.set(invUid.getMostSignificantBits());
    }

    static ClientMessage encodeRequest(int messageTypeId, RaftGroupId groupId, String name, long sessionId, long threadId) {
        int dataSize = ClientMessage.HEADER_SIZE
                + RaftGroupIdImpl.dataSize(groupId) + calculateDataSize(name) + Bits.LONG_SIZE_IN_BYTES * 2;
        ClientMessage clientMessage = prepareClientMessage(groupId, name, dataSize, messageTypeId);
        clientMessage.set(sessionId);
        clientMessage.set(threadId);
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

    public RaftGroupId getGroupId() {
        return groupId;
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

    private static class LongResponseDecoder implements ClientMessageDecoder {
        @Override
        public Long decodeClientMessage(ClientMessage clientMessage) {
            return clientMessage.getLong();
        }
    }
}
