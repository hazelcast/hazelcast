/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.impl.ClientDelegatingFuture;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CPSessionCloseSessionCodec;
import com.hazelcast.client.impl.protocol.codec.CPSessionCreateSessionCodec;
import com.hazelcast.client.impl.protocol.codec.CPSessionGenerateThreadIdCodec;
import com.hazelcast.client.impl.protocol.codec.CPSessionHeartbeatSessionCodec;
import com.hazelcast.client.impl.spi.impl.ClientInvocation;
import com.hazelcast.client.impl.spi.impl.ClientInvocationFuture;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.session.AbstractProxySessionManager;
import com.hazelcast.cp.internal.session.SessionResponse;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.InternalCompletableFuture;

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

    private final HazelcastClientInstanceImpl client;

    public ClientProxySessionManager(HazelcastClientInstanceImpl client) {
        this.client = client;
    }

    @Override
    protected long generateThreadId(RaftGroupId groupId) {
        ClientMessage request = CPSessionGenerateThreadIdCodec.encodeRequest(groupId);
        ClientMessage response = new ClientInvocation(client, request, "sessionManager").invoke().joinInternal();

        return CPSessionGenerateThreadIdCodec.decodeResponse(response);
    }

    @Override
    protected SessionResponse requestNewSession(RaftGroupId groupId) {
        ClientMessage request = CPSessionCreateSessionCodec.encodeRequest(groupId, client.getName());
        ClientMessage response = new ClientInvocation(client, request, "sessionManager").invoke().joinInternal();
        CPSessionCreateSessionCodec.ResponseParameters params = CPSessionCreateSessionCodec.decodeResponse(response);
        return new SessionResponse(params.sessionId, params.ttlMillis, params.heartbeatMillis);
    }

    @Override
    protected ScheduledFuture<?> scheduleWithRepetition(Runnable task, long period, TimeUnit unit) {
        return client.getTaskScheduler().scheduleWithRepetition(task, period, period, unit);
    }

    @Override
    protected InternalCompletableFuture<Object> heartbeat(RaftGroupId groupId, long sessionId) {
        ClientMessage request = CPSessionHeartbeatSessionCodec.encodeRequest(groupId, sessionId);
        ClientInvocationFuture future = new ClientInvocation(client, request, "sessionManager").invoke();
        return new ClientDelegatingFuture<>(future, client.getSerializationService(), clientMessage -> null);
    }

    @Override
    protected InternalCompletableFuture<Object> closeSession(RaftGroupId groupId, Long sessionId) {
        ClientMessage request = CPSessionCloseSessionCodec.encodeRequest(groupId, sessionId);
        ClientInvocationFuture future = new ClientInvocation(client, request, "sessionManager").invoke();
        return new ClientDelegatingFuture<>(future, client.getSerializationService(), CPSessionCloseSessionCodec::decodeResponse);
    }

    public void shutdownAndAwait() {
        Map<RaftGroupId, InternalCompletableFuture<Object>> futures = super.shutdown();

        ILogger logger = client.getLoggingService().getLogger(getClass());

        long remainingTimeNanos = TimeUnit.SECONDS.toNanos(SHUTDOWN_TIMEOUT_SECONDS);

        while (remainingTimeNanos > 0) {
            int closed = 0;

            for (Entry<RaftGroupId, InternalCompletableFuture<Object>> entry : futures.entrySet()) {
                CPGroupId groupId = entry.getKey();
                InternalCompletableFuture<Object> f = entry.getValue();
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
