/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.spi.properties.HazelcastProperty;

import java.nio.channels.CancelledKeyException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.concurrent.TimeUnit.SECONDS;

public abstract class AbstractListenerService {
    protected static final HazelcastProperty PUSH_PERIOD_IN_SECONDS
            = new HazelcastProperty("hazelcast.client.internal.push.period.seconds", 30, SECONDS);

    protected final Map<ClientEndpoint, Long> clientListeningEndpoints = new ConcurrentHashMap<>();
    protected final AtomicBoolean pushScheduled = new AtomicBoolean();
    protected final NodeEngine nodeEngine;
    protected final String executorName;
    protected final ILogger logger;

    public AbstractListenerService(NodeEngine nodeEngine, ILogger logger, String executorName) {
        this.logger = logger;
        this.nodeEngine = nodeEngine;
        this.executorName = executorName;
    }

    /**
     * See <a href="https://github.com/hazelcast/hazelcast-mono/pull/871#issuecomment-1983519122">
     * this comment</a> for why we do not use an acknowledgement mechanism.
     */
    protected void schedulePeriodicPush() {
        ExecutionService executor = nodeEngine.getExecutionService();
        int pushPeriodInSeconds = nodeEngine.getProperties().getSeconds(PUSH_PERIOD_IN_SECONDS);
        logger.finest("Scheduling periodic data push, on executor %s with period: %,d",
                executorName, pushPeriodInSeconds);
        if (executorName != null) {
            executor.scheduleWithRepetition(executorName, this::pushView, pushPeriodInSeconds, pushPeriodInSeconds, SECONDS);
        } else {
            executor.scheduleWithRepetition(this::pushView, pushPeriodInSeconds, pushPeriodInSeconds, SECONDS);
        }
    }

    protected abstract void pushView();

    protected void sendToListeningEndpoints(ClientMessage clientMessage) {
        for (Map.Entry<ClientEndpoint, Long> entry : clientListeningEndpoints.entrySet()) {
            Long correlationId = entry.getValue();
            // Share the partition and membership tables, copy only the initial frame.
            ClientMessage message = clientMessage.copyWithNewCorrelationId(correlationId);
            ClientEndpoint clientEndpoint = entry.getKey();
            Connection connection = clientEndpoint.getConnection();
            write(message, connection);
        }
    }

    public void registerListener(ClientEndpoint clientEndpoint, long correlationId) {
        if (pushScheduled.compareAndSet(false, true)) {
            schedulePeriodicPush();
        }
        clientListeningEndpoints.put(clientEndpoint, correlationId);
        Connection connection = clientEndpoint.getConnection();

        logger.finest("Registered listener with endpoint: " + clientEndpoint);
        sendUpdate(clientEndpoint, connection, correlationId);
    }

    protected abstract void sendUpdate(ClientEndpoint clientEndpoint, Connection connection, long correlationId);

    public void deregisterListener(ClientEndpoint clientEndpoint) {
        clientListeningEndpoints.remove(clientEndpoint);
        logger.finest("Deregistered listener with endpoint: " + clientEndpoint);
    }

    // for test purpose only
    public Map<ClientEndpoint, Long> getClusterListeningEndpoints() {
        return clientListeningEndpoints;
    }

    protected void write(ClientMessage message, Connection connection) {
        try {
            connection.write(message);
        } catch (CancelledKeyException ignored) {
            // If connection closes while writing, we can get CancelledKeyException.
            // In that case, we can safely ignore the exception.
        }
    }
}
