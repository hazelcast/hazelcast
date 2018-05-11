package com.hazelcast.raft.service.lock.client;

import com.hazelcast.client.impl.ClientMessageDecoder;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.client.spi.impl.ClientInvocationFuture;
import com.hazelcast.client.util.ClientDelegatingFuture;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.RaftGroupIdImpl;
import com.hazelcast.raft.service.lock.FencedLock;
import com.hazelcast.raft.service.lock.proxy.AbstractRaftFencedLockProxy;
import com.hazelcast.raft.service.session.SessionManagerProvider;

import java.util.UUID;
import java.util.concurrent.Future;

import static com.hazelcast.client.impl.protocol.util.ParameterUtil.calculateDataSize;
import static com.hazelcast.raft.service.lock.client.LockMessageTaskFactoryProvider.CREATE_TYPE;
import static com.hazelcast.raft.service.lock.client.LockMessageTaskFactoryProvider.FORCE_UNLOCK;
import static com.hazelcast.raft.service.lock.client.LockMessageTaskFactoryProvider.LOCK;
import static com.hazelcast.raft.service.lock.client.LockMessageTaskFactoryProvider.LOCK_COUNT;
import static com.hazelcast.raft.service.lock.client.LockMessageTaskFactoryProvider.LOCK_FENCE;
import static com.hazelcast.raft.service.lock.client.LockMessageTaskFactoryProvider.TRY_LOCK;
import static com.hazelcast.raft.service.lock.client.LockMessageTaskFactoryProvider.UNLOCK;
import static com.hazelcast.raft.service.lock.client.RaftLockProxy.BOOLEAN_RESPONSE_DECODER;
import static com.hazelcast.raft.service.lock.client.RaftLockProxy.INT_RESPONSE_DECODER;
import static com.hazelcast.raft.service.lock.client.RaftLockProxy.LONG_RESPONSE_DECODER;
import static com.hazelcast.raft.service.lock.client.RaftLockProxy.encodeRequest;
import static com.hazelcast.raft.service.lock.client.RaftLockProxy.invoke;
import static com.hazelcast.raft.service.util.ClientAccessor.getClient;

/**
 * TODO: Javadoc Pending...
 */
public class RaftFencedLockProxy extends AbstractRaftFencedLockProxy {

    public static FencedLock create(HazelcastInstance instance, String name) {
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

        RaftGroupId groupId = RaftLockProxy.join(future);
        return new RaftFencedLockProxy(instance, groupId, name);
    }

    private final HazelcastClientInstanceImpl client;

    public RaftFencedLockProxy(HazelcastInstance instance, RaftGroupId groupId, String name) {
        super(SessionManagerProvider.get(getClient(instance)), groupId, name);
        this.client = getClient(instance);
    }

    @Override
    protected Future<Long> doLock(RaftGroupId groupId, String name, long sessionId, long threadId, UUID invocationUid) {
        ClientMessage message = encodeRequest(LOCK, groupId, name, sessionId, threadId, invocationUid);
        return invoke(client, LONG_RESPONSE_DECODER, name, message);
    }

    @Override
    protected Future<Long> doTryLock(RaftGroupId groupId, String name, long sessionId, long threadId, UUID invocationUid,
                                     long timeoutMillis) {
        ClientMessage message = encodeRequest(TRY_LOCK, groupId, name, sessionId, threadId, invocationUid, timeoutMillis);
        return invoke(client, LONG_RESPONSE_DECODER, name, message);
    }

    @Override
    protected Future<Object> doUnlock(RaftGroupId groupId, String name, long sessionId, long threadId, UUID invocationUid) {
        ClientMessage message = encodeRequest(UNLOCK, groupId, name, sessionId, threadId, invocationUid);
        return invoke(client, BOOLEAN_RESPONSE_DECODER, name, message);
    }

    @Override
    protected Future<Object> doForceUnlock(RaftGroupId groupId, String name, long expectedFence, UUID invocationUid) {
        ClientMessage message = encodeRequest(FORCE_UNLOCK, groupId, name, -1, -1, invocationUid, expectedFence);
        return invoke(client, BOOLEAN_RESPONSE_DECODER, name, message);
    }

    @Override
    protected Future<Long> doGetLockFence(RaftGroupId groupId, String name) {
        ClientMessage message = encodeRequest(LOCK_FENCE, groupId, name, -1, -1);
        return invoke(client, LONG_RESPONSE_DECODER, name, message);
    }

    @Override
    protected Future<Integer> doGetLockCount(RaftGroupId groupId, String name) {
        ClientMessage message = encodeRequest(LOCK_COUNT, groupId, name, -1, -1);
        return invoke(client, INT_RESPONSE_DECODER, name, message);
    }
}
