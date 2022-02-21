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

package com.hazelcast.sql.impl;

import com.hazelcast.sql.impl.plan.cache.PlanCacheChecker;
import com.hazelcast.sql.impl.state.QueryClientStateRegistry;
import com.hazelcast.sql.impl.state.QueryResultRegistry;
import com.hazelcast.sql.impl.state.QueryStateRegistryUpdater;

/**
 * Proxy for SQL service.
 */
public class SqlInternalService {

    public static final String SERVICE_NAME = "hz:impl:sqlService";

    /** Registry for query results. */
    private final QueryResultRegistry resultRegistry;

    /** Registry for client queries. */
    private final QueryClientStateRegistry clientStateRegistry;

    /** State registry updater. */
    private final QueryStateRegistryUpdater stateRegistryUpdater;

    public SqlInternalService(
        QueryResultRegistry resultRegistry,
        String instanceName,
        NodeServiceProvider nodeServiceProvider,
        long stateCheckFrequency,
        PlanCacheChecker planCacheChecker
    ) {
        this.resultRegistry = resultRegistry;

        // Create state registries since they do not depend on anything.
        this.clientStateRegistry = new QueryClientStateRegistry();

        // State checker depends on state registries and operation handler.
        this.stateRegistryUpdater = new QueryStateRegistryUpdater(
            instanceName,
            nodeServiceProvider,
            clientStateRegistry,
            planCacheChecker,
            stateCheckFrequency
        );
    }

    public void start() {
        stateRegistryUpdater.start();
    }

    public void shutdown() {
        stateRegistryUpdater.shutdown();

        resultRegistry.shutdown();
        clientStateRegistry.shutdown();
    }

    public QueryResultRegistry getResultRegistry() {
        return resultRegistry;
    }

    public QueryClientStateRegistry getClientStateRegistry() {
        return clientStateRegistry;
    }

    /**
     * For testing only.
     */
    public QueryStateRegistryUpdater getStateRegistryUpdater() {
        return stateRegistryUpdater;
    }
}
