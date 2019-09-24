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

package com.hazelcast.client.cp.internal.session;

import com.hazelcast.client.impl.clientside.ClientMessageDecoder;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CPSessionCloseSessionCodec;
import com.hazelcast.client.impl.protocol.codec.CPSessionCreateSessionCodec;
import com.hazelcast.client.impl.protocol.codec.CPSessionGenerateThreadIdCodec;
import com.hazelcast.client.impl.protocol.codec.CPSessionHeartbeatSessionCodec;
import com.hazelcast.client.impl.spi.impl.ClientInvocation;
import com.hazelcast.client.impl.spi.impl.ClientInvocationFuture;
import com.hazelcast.client.impl.ClientDelegatingFuture;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.session.AbstractProxySessionManager;
import com.hazelcast.cp.internal.session.SessionResponse;
import com.hazelcast.logging.ILogger;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Client-side implementation of Raft proxy session manager
 */
public class ClientProxySessionManager extends AbstractProxySessionManager {

    private static final long SHUTDOWN_TIMEOUT_SECONDS = 60;
    private static final long SHUTDOWN_WAIT_SLEEP_MILLIS = 10;

    private static final ClientMessageDecoder HEARTBEAT_RESPONSE_DECODER = clientMessage -> null;

    private static final ClientMessageDecoder CLOSE_SESSION_RESPONSE_DECODER =
            clientMessage -> CPSessionCloseSessionCodec.decodeResponse(clientMessage).response;


    private final HazelcastClientInstanceImpl client;

    public ClientProxySessionManager(HazelcastClientInstanceImpl client) {
        this.client = client;
    }

    @Override
    protected long generateThreadId(RaftGroupId groupId) {
        ClientMessage request = CPSessionGenerateThreadIdCodec.encodeRequest(groupId);
        ClientMessage response = new ClientInvocation(client, request, "sessionManager").invoke().join();

        return CPSessionGenerateThreadIdCodec.decodeResponse(response).response;
    }

    @Override
    protected SessionResponse requestNewSession(RaftGroupId groupId) {
        ClientMessage request = CPSessionCreateSessionCodec.encodeRequest(groupId, client.getName());
        ClientMessage response = new ClientInvocation(client, request, "sessionManager").invoke().join();
        CPSessionCreateSessionCodec.ResponseParameters params = CPSessionCreateSessionCodec.decodeResponse(response);
        return new SessionResponse(params.sessionId, params.ttlMillis, params.heartbeatMillis);
    }

    @Override
    protected ScheduledFuture<?> scheduleWithRepetition(Runnable task, long period, TimeUnit unit) {
        return client.getClientExecutionService().scheduleWithRepetition(task, period, period, unit);
    }

    @Override
    protected ICompletableFuture<Object> heartbeat(RaftGroupId groupId, long sessionId) {
        ClientMessage request = CPSessionHeartbeatSessionCodec.encodeRequest(groupId, sessionId);
        ClientInvocationFuture future = new ClientInvocation(client, request, "sessionManager").invoke();
        return new ClientDelegatingFuture<>(future, client.getSerializationService(), HEARTBEAT_RESPONSE_DECODER);
    }

    @Override
    protected ICompletableFuture<Object> closeSession(RaftGroupId groupId, Long sessionId) {
        ClientMessage request = CPSessionCloseSessionCodec.encodeRequest(groupId, sessionId);
        ClientInvocationFuture future = new ClientInvocation(client, request, "sessionManager").invoke();
        return new ClientDelegatingFuture<>(future, client.getSerializationService(), CLOSE_SESSION_RESPONSE_DECODER);
    }

    public void shutdownAndAwait() {
        Map<RaftGroupId, ICompletableFuture<Object>> futures = shutdown();

        ILogger logger = client.getLoggingService().getLogger(getClass());

        long remainingTimeNanos = TimeUnit.SECONDS.toNanos(SHUTDOWN_TIMEOUT_SECONDS);

        while (remainingTimeNanos > 0) {
            int closed = 0;

            for (Entry<RaftGroupId, ICompletableFuture<Object>> entry : futures.entrySet()) {
                CPGroupId groupId = entry.getKey();
                ICompletableFuture<Object> f = entry.getValue();
                if (f.isDone()) {
                    closed++;
                    try {
                        f.get();
                        logger.fine("Session closed for " + groupId);
                    } catch (Exception e) {
                        logger.warning("Close session failed for " + groupId, e);

                    }
                }
            }

            if (closed == futures.size()) {
                break;
            }

            try {
                Thread.sleep(SHUTDOWN_WAIT_SLEEP_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }

            remainingTimeNanos -= MILLISECONDS.toNanos(SHUTDOWN_WAIT_SLEEP_MILLIS);
        }
    }

}
