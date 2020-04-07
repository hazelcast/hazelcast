/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.state;

import com.hazelcast.sql.impl.NodeServiceProvider;
import com.hazelcast.sql.impl.client.QueryClientStateRegistry;
import com.hazelcast.sql.impl.operation.QueryOperationHandler;

import java.util.Collection;
import java.util.UUID;

/**
 * Class performing periodic query state check.
 */
public class QueryStateRegistryUpdater {
    /** Node service provider. */
    private final NodeServiceProvider nodeServiceProvider;

    /** State to be checked. */
    private final QueryStateRegistry stateRegistry;

    /** Client state registry. */
    private final QueryClientStateRegistry clientStateRegistry;

    /** Operation handler. */
    private final QueryOperationHandler operationHandler;

    /** State check frequency. */
    private final long stateCheckFrequency;

    /** Worker performing periodic state check. */
    private final Worker worker = new Worker();

    public QueryStateRegistryUpdater(
        NodeServiceProvider nodeServiceProvider,
        QueryStateRegistry stateRegistry,
        QueryClientStateRegistry clientStateRegistry,
        QueryOperationHandler operationHandler,
        long stateCheckFrequency
    ) {
        if (stateCheckFrequency <= 0) {
            throw new IllegalArgumentException("State check frequency must be positive: " + stateCheckFrequency);
        }

        this.nodeServiceProvider = nodeServiceProvider;
        this.stateRegistry = stateRegistry;
        this.clientStateRegistry = clientStateRegistry;
        this.operationHandler = operationHandler;
        this.stateCheckFrequency = stateCheckFrequency;
    }

    public void start() {
        worker.start();
    }

    public void stop() {
        worker.stop();
    }

    private class Worker implements Runnable {
        private final Object startMux = new Object();
        private volatile Thread thread;
        private volatile boolean stopped;

        public void start() {
            synchronized (startMux) {
                if (stopped || thread != null) {
                    return;
                }

                Thread thread = new Thread(this);

                thread.setName("sql-query-state-checker");
                thread.setDaemon(true);

                thread.start();

                this.thread = thread;
            }
        }

        @Override
        public void run() {
            while (!stopped) {
                try {
                    Thread.sleep(stateCheckFrequency);

                    checkMemberState();
                    checkClientState();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();

                    break;
                }
            }
        }

        private void checkMemberState() {
            UUID localMemberID = nodeServiceProvider.getLocalMemberId();
            Collection<UUID> activeMemberIds = nodeServiceProvider.getDataMemberIds();

            stateRegistry.update(localMemberID, activeMemberIds, operationHandler);
        }

        private void checkClientState() {
            Collection<UUID> activeClientIds = nodeServiceProvider.getClientMembersIds();

            clientStateRegistry.update(activeClientIds);
        }

        public void stop() {
            synchronized (startMux) {
                if (stopped) {
                    return;
                }

                stopped = true;

                if (thread != null) {
                    thread.interrupt();
                }
            }
        }
    }
}
