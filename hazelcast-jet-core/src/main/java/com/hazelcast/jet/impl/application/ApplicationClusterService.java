/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.application;

import com.hazelcast.jet.config.ApplicationConfig;
import com.hazelcast.jet.container.CounterKey;
import com.hazelcast.jet.counters.Accumulator;
import com.hazelcast.jet.dag.DAG;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;

/**
 * Abstract service for JET-application's management
 */
public interface ApplicationClusterService {
    /**
     * Init application
     *
     * @param config                  -   application's config
     * @param applicationStateManager -
     */
    void initApplication(ApplicationConfig config,
                         ApplicationStateManager applicationStateManager
    );

    /**
     * Submits dag for the application
     *
     * @param dag                     - direct acyclic graph
     * @param applicationStateManager - manager to work with application state-machine
     */
    void submitDag(DAG dag, ApplicationStateManager applicationStateManager);

    /**
     * Performs localization operation
     *
     * @param localizedResources      -   classpath resources
     * @param applicationStateManager - manager to work with application state-machine
     */
    void localizeApplication(Set<LocalizationResource> localizedResources,
                             ApplicationStateManager applicationStateManager);

    /**
     * Execute application
     *
     * @param applicationStateManager - manager to work with application state-machine
     * @return - awaiting Future
     */
    Future executeApplication(ApplicationStateManager applicationStateManager);

    /**
     * Interrupt application
     *
     * @param applicationStateManager - manager to work with application state-machine
     * @return - awaiting Future
     */
    Future interruptApplication(ApplicationStateManager applicationStateManager);

    /**
     * Finalize application
     *
     * @param applicationStateManager - manager to work with application state-machine
     * @return - awaiting Future
     */
    Future finalizeApplication(ApplicationStateManager applicationStateManager);

    /**
     * @return accumulators
     */
    Map<CounterKey, Accumulator> getAccumulators();
}
