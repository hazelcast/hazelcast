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

package com.hazelcast.internal.networking.nio.iobalancer;

import com.hazelcast.internal.networking.nio.MigratablePipeline;
import com.hazelcast.internal.networking.nio.NioThread;

/**
 * Decides if a {@link MigratablePipeline pipeline} migration should be attempted
 * and which pipeline to choose.
 *
 * @see IOBalancer
 */
interface MigrationStrategy {

    /**
     * Looks for imbalance in {@link MigratablePipeline pipeline} to {@link NioThread ioThread}
     * mapping.
     *
     * @param imbalance
     * @return <code>true</code> when imbalance is detected
     */
    boolean imbalanceDetected(LoadImbalance imbalance);

    /**
     * Finds a {@link MigratablePipeline pipeline} to migrate
     *
     * @param imbalance
     * @return Handler to migrate or <code>null</code> if no suitable candidate is found
     */
    MigratablePipeline findPipelineToMigrate(LoadImbalance imbalance);
}
