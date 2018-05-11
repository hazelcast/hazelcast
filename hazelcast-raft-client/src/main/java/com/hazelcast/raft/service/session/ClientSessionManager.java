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

package com.hazelcast.raft.service.session;

import com.hazelcast.client.impl.ClientMessageDecoder;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.client.spi.impl.ClientInvocationFuture;
import com.hazelcast.client.util.ClientDelegatingFuture;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.nio.Bits;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.RaftGroupIdImpl;
import com.hazelcast.raft.impl.session.SessionResponse;
import com.hazelcast.raft.service.session.client.SessionMessageTaskFactoryProvider;
import com.hazelcast.util.ExceptionUtil;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * TODO: Javadoc Pending...
 *
 */
// TODO [basri] integrate shutdown() to graceful shutdown of the client
public class ClientSessionManager extends AbstractSessionManager {

    private static final ClientMessageDecoder SESSION_RESPONSE_DECODER = new SessionResponseDecoder();
    private static final ClientMessageDecoder BOOLEAN_RESPONSE_DECODER = new BooleanResponseDecoder();

    private final HazelcastClientInstanceImpl client;

    public ClientSessionManager(HazelcastClientInstanceImpl client) {
        this.client = client;
    }

    @Override
    protected SessionResponse requestNewSession(RaftGroupId groupId) {
        int dataSize = ClientMessage.HEADER_SIZE + RaftGroupIdImpl.dataSize(groupId);
        ClientMessage clientMessage = ClientMessage.createForEncode(dataSize);
        clientMessage.setMessageType(SessionMessageTaskFactoryProvider.CREATE);
        clientMessage.setRetryable(false);
        clientMessage.setOperationName("");
        RaftGroupIdImpl.writeTo(groupId, clientMessage);
        clientMessage.updateFrameLength();

        ICompletableFuture<SessionResponse> future = invoke(clientMessage, SESSION_RESPONSE_DECODER);
        return join(future);
    }

    @Override
    protected ScheduledFuture<?> scheduleWithRepetition(Runnable task, long period, TimeUnit unit) {
        return client.getClientExecutionService().scheduleWithRepetition(task, period, period, unit);
    }

    @Override
    protected ICompletableFuture<Object> heartbeat(RaftGroupId groupId, long sessionId) {
        int dataSize = ClientMessage.HEADER_SIZE + RaftGroupIdImpl.dataSize(groupId) + Bits.LONG_SIZE_IN_BYTES;
        ClientMessage clientMessage = ClientMessage.createForEncode(dataSize);
        clientMessage.setMessageType(SessionMessageTaskFactoryProvider.HEARTBEAT);
        clientMessage.setRetryable(false);
        clientMessage.setOperationName("");
        RaftGroupIdImpl.writeTo(groupId, clientMessage);
        clientMessage.set(sessionId);
        clientMessage.updateFrameLength();

        return invoke(clientMessage, BOOLEAN_RESPONSE_DECODER);
    }

    @Override
    protected ICompletableFuture<Object> closeSession(RaftGroupId groupId, Long sessionId) {
        int dataSize = ClientMessage.HEADER_SIZE + RaftGroupIdImpl.dataSize(groupId) + Bits.LONG_SIZE_IN_BYTES;
        ClientMessage clientMessage = ClientMessage.createForEncode(dataSize);
        clientMessage.setMessageType(SessionMessageTaskFactoryProvider.CLOSE_SESSION);
        clientMessage.setRetryable(false);
        clientMessage.setOperationName("");
        RaftGroupIdImpl.writeTo(groupId, clientMessage);
        clientMessage.set(sessionId);
        clientMessage.updateFrameLength();

        return invoke(clientMessage, BOOLEAN_RESPONSE_DECODER);
    }

    private <T> ICompletableFuture<T> invoke(ClientMessage clientMessage, ClientMessageDecoder decoder) {
        ClientInvocationFuture future = new ClientInvocation(client, clientMessage, "session").invoke();
        return new ClientDelegatingFuture<T>(future, client.getSerializationService(), decoder);
    }

    private static <T> T join(ICompletableFuture<T> future) {
        try {
            return future.get();
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    private static class BooleanResponseDecoder implements ClientMessageDecoder {
        @Override
        public Boolean decodeClientMessage(ClientMessage clientMessage) {
            return clientMessage.getBoolean();
        }
    }

    private static class SessionResponseDecoder implements ClientMessageDecoder {
        @Override
        public SessionResponse decodeClientMessage(ClientMessage clientMessage) {
            long sessionId = clientMessage.getLong();
            long sessionTTL = clientMessage.getLong();
            long heartbeatInterval = clientMessage.getLong();
            return new SessionResponse(sessionId, sessionTTL, heartbeatInterval);
        }
    }
}
