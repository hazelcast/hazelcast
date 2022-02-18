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

import java.util.Set;

/**
 * Default {@link MigrationStrategy} for {@link MigratablePipeline} instances.
 *
 * It attempts to trigger a migration if a ratio between least busy and most
 * busy IOThreads exceeds {@link #MIN_MAX_RATIO_MIGRATION_THRESHOLD}.
 *
 * Once a migration is triggered it tries to find the busiest pipeline registered in
 * {@link LoadImbalance#srcOwner} which wouldn't cause overload of the
 * {@link LoadImbalance#dstOwner} after a migration.
 */
class LoadMigrationStrategy implements MigrationStrategy {

    /**
     * You can use this property to tune whether the migration will be attempted
     * at all. The higher the number is the more likely the migration will be
     * attempted. Too higher number will result in unnecessary overhead, too
     * low number will cause performance degradation due selector imbalance.
     *
     * Try to schedule a migration if the least busy NioThread receives less loads
     * then (MIN_MAX_RATIO_MIGRATION_THRESHOLD * no. of load received by the busiest
     * NioThread)
     */
    private static final double MIN_MAX_RATIO_MIGRATION_THRESHOLD = 0.8;

    /**
     * You can use this property to tune a selection process for pipeline migration.
     * The higher number is the more aggressive migration process is.
     */
    private static final double MAXIMUM_NO_OF_EVENTS_AFTER_MIGRATION_COEFFICIENT = 0.9;

    /**
     * Checks if an imbalance was detected in the system
     *
     * @param imbalance
     * @return <code>true</code> if imbalance threshold has been reached and migration
     * should be attempted
     */
    @Override
    public boolean imbalanceDetected(LoadImbalance imbalance) {
        long min = imbalance.minimumLoad;
        long max = imbalance.maximumLoad;

        if (min == Long.MIN_VALUE || max == Long.MAX_VALUE) {
            return false;
        }
        long lowerBound = (long) (MIN_MAX_RATIO_MIGRATION_THRESHOLD * max);
        return min < lowerBound;
    }

    /**
     * Attempt to find a pipeline to migrate to a new NioThread.
     *
     * @param imbalance describing a snapshot of NioThread load
     * @return the pipeline to migrate to a new NioThread or null if no
     * pipeline needs to be migrated.
     */
    @Override
    public MigratablePipeline findPipelineToMigrate(LoadImbalance imbalance) {
        Set<? extends MigratablePipeline> candidates = imbalance.getPipelinesOwnedBy(imbalance.srcOwner);
        long migrationThreshold = (long) ((imbalance.maximumLoad - imbalance.minimumLoad)
                * MAXIMUM_NO_OF_EVENTS_AFTER_MIGRATION_COEFFICIENT);
        MigratablePipeline candidate = null;
        long loadInSelectedPipeline = 0;
        for (MigratablePipeline pipeline : candidates) {
            long load = imbalance.getLoad(pipeline);
            if (load > loadInSelectedPipeline) {
                if (load < migrationThreshold) {
                    loadInSelectedPipeline = load;
                    candidate = pipeline;
                }
            }
        }
        return candidate;
    }
}
