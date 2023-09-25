/*
 * Copyright 2023 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.sql.impl.state;

import com.hazelcast.jet.function.RunnableEx;
import com.hazelcast.jet.impl.JetServiceBackend;
import com.hazelcast.logging.ILogger;
import com.hazelcast.sql.impl.DataConnectionConsistencyChecker;
import com.hazelcast.sql.impl.NodeServiceProvider;
import com.hazelcast.sql.impl.QueryUtils;
import com.hazelcast.sql.impl.plan.cache.PlanCacheChecker;

import java.util.Set;
import java.util.UUID;

import static com.hazelcast.sql.impl.QueryUtils.WORKER_TYPE_STATE_CHECKER;

/**
 * Class performing periodic query state check.
 */
public class QueryStateRegistryUpdater {

    private final NodeServiceProvider nodeServiceProvider;
    private final QueryClientStateRegistry clientStateRegistry;
    private final PlanCacheChecker planCacheChecker;
    private final DataConnectionConsistencyChecker dataConnectionConsistencyChecker;

    private final ILogger logger;

    /**
     * "volatile" instead of "final" only to allow for value change from unit tests.
     */
    private volatile long stateCheckFrequency;

    /**
     * Worker performing periodic state check.
     */
    private final Worker worker;

    public QueryStateRegistryUpdater(
            String instanceName,
            NodeServiceProvider nodeServiceProvider,
            QueryClientStateRegistry clientStateRegistry,
            PlanCacheChecker planCacheChecker,
            DataConnectionConsistencyChecker dataConnectionConsistencyChecker,
            long stateCheckFrequency
    ) {
        if (stateCheckFrequency <= 0) {
            throw new IllegalArgumentException("State check frequency must be positive: " + stateCheckFrequency);
        }

        this.nodeServiceProvider = nodeServiceProvider;
        this.clientStateRegistry = clientStateRegistry;
        this.planCacheChecker = planCacheChecker;
        this.dataConnectionConsistencyChecker = dataConnectionConsistencyChecker;
        this.stateCheckFrequency = stateCheckFrequency;
        this.logger = nodeServiceProvider.getLogger(getClass());

        worker = new Worker(instanceName);
    }

    public void start() {
        worker.start();
    }

    public void shutdown() {
        worker.stop();
    }

    /**
     * For testing only.
     */
    public void setStateCheckFrequency(long stateCheckFrequency) {
        this.stateCheckFrequency = stateCheckFrequency;

        worker.thread.interrupt();
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
                long currentStateCheckFrequency = stateCheckFrequency;

                try {
                    Thread.sleep(currentStateCheckFrequency);

                    executeInterruptibly(this::checkClientState);
                    executeInterruptibly(this::checkPlans);
                    executeInterruptibly(this::checkDataConnectionsConsistency);
                } catch (InterruptedException e) {
                    if (currentStateCheckFrequency != stateCheckFrequency) {
                        // Interrupted due to frequency change.
                        continue;
                    }

                    Thread.currentThread().interrupt();

                    break;
                }
            }
        }

        private void executeInterruptibly(RunnableEx callback) throws InterruptedException {
            try {
                callback.runEx();
            } catch (InterruptedException i) {
                // propagate, will be handled higher up
                throw i;
            } catch (Throwable t) {
                logger.warning("Unexpected error when invoking query state registry updater action", t);
            }
        }

        private void checkClientState() {
            Set<UUID> activeClientIds = nodeServiceProvider.getClientIds();

            clientStateRegistry.update(activeClientIds);
        }

        private void checkPlans() {
            if (planCacheChecker != null) {
                planCacheChecker.check();
            }
        }

        private void checkDataConnectionsConsistency() {
            if (dataConnectionConsistencyChecker.isInitialized()) {
                try {
                    dataConnectionConsistencyChecker.check();
                } catch (Throwable t) {
                    logger.warning(t);
                }
            } else {
                if (nodeServiceProvider.getMap(JetServiceBackend.SQL_CATALOG_MAP_NAME) == null) {
                    return;
                }
                if (!dataConnectionConsistencyChecker.isInitialized()) {
                    dataConnectionConsistencyChecker.init();
                }
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
