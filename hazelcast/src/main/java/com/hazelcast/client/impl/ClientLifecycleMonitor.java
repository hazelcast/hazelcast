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

package com.hazelcast.client.impl;

import com.hazelcast.internal.services.ClientAwareService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.spi.properties.HazelcastProperties;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Monitors client connections across cluster.
 * If no connection left for configured amount of time we release the locks
 */
public class ClientLifecycleMonitor implements Runnable {

    private final long checkMillis;
    private final long timeoutMillis;
    private final ClientEndpointManager clientEndpointManager;
    private final ExecutionService executionService;
    private final NodeEngine nodeEngine;
    private final ClientEngineImpl clientEngine;
    private final ILogger logger;
    private final Map<UUID, Long> lastLiveTime = new ConcurrentHashMap<>();

    ClientLifecycleMonitor(ClientEndpointManager clientEndpointManager,
                           ClientEngineImpl clientEngine,
                           ILogger logger,
                           NodeEngine nodeEngine,
                           ExecutionService executionService,
                           HazelcastProperties hazelcastProperties) {
        this.clientEndpointManager = clientEndpointManager;
        this.logger = logger;
        this.executionService = executionService;
        this.nodeEngine = nodeEngine;
        this.clientEngine = clientEngine;
        checkMillis = hazelcastProperties.getPositiveMillisOrDefault(ClusterProperty.CLIENT_CLEANUP_PERIOD);
        timeoutMillis = hazelcastProperties.getSeconds(ClusterProperty.CLIENT_CLEANUP_TIMEOUT);

    }

    public void start() {
        executionService.scheduleWithRepetition(this, checkMillis, checkMillis, MILLISECONDS);
    }

    void addClientToMonitor(UUID uuid) {
        lastLiveTime.put(uuid, System.currentTimeMillis());
    }

    @Override
    public void run() {
        Set<UUID> allClients = null;
        Set<UUID> localClients = clientEndpointManager.getLocalClientUuids();
        for (Map.Entry<UUID, Long> entry : lastLiveTime.entrySet()) {

            UUID clientUuid = entry.getKey();

            if (localClients.contains(clientUuid)) {
                //This member has a connection to client, update the time and continue
                lastLiveTime.put(clientUuid, System.currentTimeMillis());
                continue;
            }

            long millisSinceLastLive = System.currentTimeMillis() - entry.getValue();
            if (millisSinceLastLive > timeoutMillis) {
                if (allClients == null) {
                    allClients = clientEngine.getClientsInCluster().keySet();
                }
                //A member has connection to client, update the time and continue
                if (allClients.contains(clientUuid)) {
                    lastLiveTime.put(clientUuid, System.currentTimeMillis());
                    continue;
                }

                //No one have connection left to this client, clean the resources
                logger.warning("No connection left to client cluster wide " + entry.getKey()
                        + " for " + millisSinceLastLive + " millis," + " cleaning resources of the client");
                lastLiveTime.remove(entry.getKey(), entry.getValue());
                // This part cleans up locks conditions semaphore etc..
                Collection<ClientAwareService> services = nodeEngine.getServices(ClientAwareService.class);
                for (ClientAwareService service : services) {
                    service.clientDisconnected(clientUuid);
                }

            }
        }
    }

}
