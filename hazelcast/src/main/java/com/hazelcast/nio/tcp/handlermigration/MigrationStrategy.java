/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.nio.tcp.handlermigration;

import com.hazelcast.nio.tcp.MigratableHandler;

/**
 * Decides if a {@link MigratableHandler handler} migration should be attempted
 * and which handler to choose.
 *
 * @see IOBalancer
 *
 */
interface MigrationStrategy {

    /**
     * Looks for imbalance in {@link MigratableHandler handler} to {@link com.hazelcast.nio.tcp.IOSelector ioSelector}
     * mapping.
     *
     * @param imbalance
     * @return <code>true</code> when imbalance is detected
     */
    boolean imbalanceDetected(LoadImbalance imbalance);

    /**
     * Finds a {@link MigratableHandler handler} to migrate
     *
     * @param imbalance
     * @return Handler to migrate or <code>null</code> if no suitable candidate is found
     */
    MigratableHandler findHandlerToMigrate(LoadImbalance imbalance);
}
