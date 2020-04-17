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
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.QueryUtils;
import com.hazelcast.sql.impl.operation.QueryCheckOperation;
import com.hazelcast.sql.impl.operation.QueryOperationHandler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static com.hazelcast.sql.impl.QueryUtils.WORKER_TYPE_STATE_CHECKER;

/**
 * Class performing periodic query state check.
 */
public class QueryStateRegistryUpdater {
    /** Node service provider. */
    private final NodeServiceProvider nodeServiceProvider;

    /** State to be checked. */
    private final QueryStateRegistry stateRegistry;

    /** Operation handler. */
    private final QueryOperationHandler operationHandler;

    /** State check frequency. */
    private final long stateCheckFrequency;

    /** Worker performing periodic state check. */
    private final Worker worker;

    public QueryStateRegistryUpdater(
        String instanceName,
        NodeServiceProvider nodeServiceProvider,
        QueryStateRegistry stateRegistry,
        QueryOperationHandler operationHandler,
        long stateCheckFrequency
    ) {
        if (stateCheckFrequency <= 0) {
            throw new IllegalArgumentException("State check frequency must be positive: " + stateCheckFrequency);
        }

        this.nodeServiceProvider = nodeServiceProvider;
        this.stateRegistry = stateRegistry;
        this.operationHandler = operationHandler;
        this.stateCheckFrequency = stateCheckFrequency;

        worker = new Worker(instanceName);
    }

    public void start() {
        worker.start();
    }

    public void stop() {
        worker.stop();
    }

    private final class Worker implements Runnable {
        private final Object startMux = new Object();
        private final String instanceName;
        private Thread thread;
        private volatile boolean stopped;

        private Worker(String instanceName) {
            this.instanceName = instanceName;
        }

        public void start() {
            synchronized (startMux) {
                if (stopped || thread != null) {
                    return;
                }

                Thread thread = new Thread(this);

                thread.setName(QueryUtils.workerName(instanceName, WORKER_TYPE_STATE_CHECKER));
                thread.setDaemon(true);

                thread.start();

                this.thread = thread;
            }
        }

        @SuppressWarnings("BusyWait")
        @Override
        public void run() {
            while (!stopped) {
                try {
                    Thread.sleep(stateCheckFrequency);

                    checkMemberState();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();

                    break;
                }
            }
        }

        private void checkMemberState() {
            Collection<UUID> activeMemberIds = nodeServiceProvider.getDataMemberIds();

            Map<UUID, Collection<QueryId>> checkMap = new HashMap<>();

            for (QueryState state : stateRegistry.getStates()) {
                // 1. Check if the query has timed out.
                if (state.tryCancelOnTimeout()) {
                    continue;
                }

                // 2. Check whether the member required for the query has left.
                if (state.tryCancelOnMemberLeave(activeMemberIds)) {
                    continue;
                }

                // 3. Check whether the query is not initialized for too long. If yes, trigger check process.
                if (state.requestQueryCheck(stateCheckFrequency)) {
                    QueryId queryId = state.getQueryId();

                    checkMap.computeIfAbsent(queryId.getMemberId(), (key) -> new ArrayList<>(1)).add(queryId);
                }
            }

            // 4. Send batched check requests.
            UUID localMemberId = nodeServiceProvider.getLocalMemberId();

            for (Map.Entry<UUID, Collection<QueryId>> checkEntry : checkMap.entrySet()) {
                QueryCheckOperation operation = new QueryCheckOperation(checkEntry.getValue());

                operationHandler.submit(localMemberId, checkEntry.getKey(), operation);
            }
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
