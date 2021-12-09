/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.client.impl.spi.impl;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ClientLocalBackupListenerCodec;
import com.hazelcast.client.impl.spi.ClientListenerService;
import com.hazelcast.client.impl.spi.EventHandler;
import com.hazelcast.client.impl.spi.invocation.ClientInvocation;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.executionservice.TaskScheduler;

import java.util.Map;
import java.util.UUID;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Coordinates all the logic related to backup events.
 * When a IMap.put is done it sends backup operations to other members.
 * For sync backups, we need to wait for backups to be get completed before returning response to the user.
 * There are two ways we are handling this:
 * <p>
 * First one is when the client is smart {@link ClientNetworkConfig#isSmartRouting()} and
 * {@link ClientConfig#isBackupAckToClientEnabled()}. In this case, this service is utilized.
 * The member is told to not wait for backup events and send backup events to the client instead of the member
 * via a flag on the client message. {@link #markMessageAsBackupAware}
 * We add backup listeners to all the members on the cluster.
 * The client knows how many backup its needs to wait via the incoming original response of map.put.
 * Incoming events are redirected to the {@link ClientInvocation} so that the waiting invocation can be freed.
 * Additionally, we periodically check all the invocations to free them after `operationBackupTimeoutMillis`
 * see {@link com.hazelcast.client.properties.ClientProperty#OPERATION_BACKUP_TIMEOUT_MILLIS}
 * <p>
 * In the second case, either the client is not smart or {@link ClientConfig#isBackupAckToClientEnabled()} is false.
 * In this case, the backup events are waited on the member that map.put first arrives.
 * Incoming response of map.put reports that there are no backups to wait for the clients.
 * The client does not wait for extra backups.
 * <p>
 * The first case is much more performant and the default behaviour. It is more performant because there are
 * only 3 network hops in the longest path that needs to be waited, whereas in the second case there are 4 hops.
 * First case 3 network hops as follows:
 * 1. Client sends map.put to member.
 * 2. Member sends backup operation to members in parallel. Also sends the response back to client in parallel.
 * 3. Backup member sends the backup event to the client.
 * <p>
 * Second case 4 network hops are as follows:
 * 1. Client sends map.put to member.
 * 2. Member sends backup operation to members in parallel.
 * 3. Backup member sends the backup event to the first member back.
 * 4. First member sends response back the client after getting the backup events from other members.
 */
public final class ClientBackupEventService {

    private static final ListenerMessageCodec BACKUP_LISTENER = new ListenerMessageCodec() {
        @Override
        public ClientMessage encodeAddRequest(boolean localOnly) {
            return ClientLocalBackupListenerCodec.encodeRequest();
        }

        @Override
        public UUID decodeAddResponse(ClientMessage clientMessage) {
            return ClientLocalBackupListenerCodec.decodeResponse(clientMessage);
        }

        @Override
        public ClientMessage encodeRemoveRequest(UUID realRegistrationId) {
            return null;
        }

        @Override
        public boolean decodeRemoveResponse(ClientMessage clientMessage) {
            return false;
        }
    };

    private ClientBackupEventService() {

    }

    public static void startBackupTask(Map<Long, ClientInvocation> unmodifiableInvocationsMap,
                                       TaskScheduler taskScheduler,
                                       long cleanResourcesMillis,
                                       int operationBackupTimeoutMillis) {
        BackupTimeoutTask task = new BackupTimeoutTask(unmodifiableInvocationsMap, operationBackupTimeoutMillis);
        taskScheduler.scheduleWithRepetition(task, cleanResourcesMillis, cleanResourcesMillis, MILLISECONDS);
    }

    private static class BackupTimeoutTask implements Runnable {

        private final Map<Long, ClientInvocation> unmodifiableInvocationsMap;
        private final int operationBackupTimeoutMillis;

        BackupTimeoutTask(Map<Long, ClientInvocation> unmodifiableInvocationsMap,
                          int operationBackupTimeoutMillis) {
            this.unmodifiableInvocationsMap = unmodifiableInvocationsMap;
            this.operationBackupTimeoutMillis = operationBackupTimeoutMillis;
        }

        @Override
        public void run() {
            for (ClientInvocation invocation : unmodifiableInvocationsMap.values()) {
                invocation.detectAndHandleBackupTimeout(operationBackupTimeoutMillis);
            }
        }
    }

    public static void addBackupListener(ClientListenerService listenerService,
                                         Map<Long, ClientInvocation> unmodifiableInvocationsMap,
                                         ILogger logger) {

        listenerService.registerListener(BACKUP_LISTENER, new BackupEventHandler(unmodifiableInvocationsMap, logger));

    }


    static class BackupEventHandler extends ClientLocalBackupListenerCodec.AbstractEventHandler
            implements EventHandler<ClientMessage> {

        private final Map<Long, ClientInvocation> unmodifiableInvocationsMap;
        private final ILogger logger;

        BackupEventHandler(Map<Long, ClientInvocation> unmodifiableInvocationsMap, ILogger logger) {
            this.unmodifiableInvocationsMap = unmodifiableInvocationsMap;
            this.logger = logger;
        }

        @Override
        public void handleBackupEvent(long sourceInvocationCorrelationId) {
            ClientInvocation invocation = unmodifiableInvocationsMap.get(sourceInvocationCorrelationId);
            if (invocation == null) {
                if (logger.isFinestEnabled()) {
                    logger.finest("Invocation not found for backup event, invocation id "
                            + sourceInvocationCorrelationId);
                }
                return;
            }
            invocation.notifyBackupComplete();
        }
    }
}
