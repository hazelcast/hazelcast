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

package com.hazelcast.nio.tcp.iobalancer;

import com.hazelcast.nio.tcp.MigratableHandler;
import java.util.Set;

/**
 * Default {@link MigrationStrategy} for {@link MigratableHandler} instances.
 *
 * It attempts to trigger a migration if a ratio between least busy and most busy selectors
 * exceeds {@link #MIN_MAX_RATIO_MIGRATION_THRESHOLD}.
 *
 * Once a migration is triggered it tries to find the busiest handler registered in
 * {@link LoadImbalance#sourceSelector} which wouldn't cause
 * overload of the {@link LoadImbalance#destinationSelector} after a migration.
 *
 */
class EventCountBasicMigrationStrategy implements MigrationStrategy {

    /**
     * You can use this property to tune whether the migration will be attempted at all. The higher the number is
     * the more likely the migration will be attempted. Too higher number will result in unnecessary overhead, too
     * low number will cause performance degradation due selector imbalance.
     *
     * Try to schedule a migration if the least busy IOSelector receives less events
     * then (MIN_MAX_RATIO_MIGRATION_THRESHOLD * no. of events received by the busiest IOSelector)
     *
     */
    private static final double MIN_MAX_RATIO_MIGRATION_THRESHOLD = 0.8;

    /**
     * You can use this property to tune a selection process for handler migration. The higher number is the more
     * aggressive migration process is.
     */
    private static final double MAXIMUM_NO_OF_EVENTS_AFTER_MIGRATION_COEFFICIENT = 0.9;

    /**
     * Checks if an imbalance was detected in the system
     *
     * @param imbalance
     * @return <code>true</code> if imbalance threshold has been reached and migration should be attempted
     */
    @Override
    public boolean imbalanceDetected(LoadImbalance imbalance) {
        long min = imbalance.minimumEvents;
        long max = imbalance.maximumEvents;

        if (min == Long.MIN_VALUE || max == Long.MAX_VALUE) {
            return false;
        }
        long lowerBound = (long) (MIN_MAX_RATIO_MIGRATION_THRESHOLD * max);
        return min < lowerBound;
    }

    /**
     * Attempt to find a handler to migrate to a new IOSelector.
     *
     * @param imbalance describing a snapshot of IOSelector load
     * @return the handler to migrate to a new IOSelector or null if no handler needs to be migrated.
     */
    @Override
    public MigratableHandler findHandlerToMigrate(LoadImbalance imbalance) {
        Set<? extends MigratableHandler> candidates = imbalance.getHandlersOwnerBy(imbalance.sourceSelector);
        long migrationThreshold = (long) ((imbalance.maximumEvents - imbalance.minimumEvents)
                * MAXIMUM_NO_OF_EVENTS_AFTER_MIGRATION_COEFFICIENT);
        MigratableHandler candidate = null;
        long eventCountInSelectedHandler = 0;
        for (MigratableHandler handler : candidates) {
            long eventCount = imbalance.getEventCount(handler);
            if (eventCount > eventCountInSelectedHandler) {
                if (eventCount < migrationThreshold) {
                    eventCountInSelectedHandler = eventCount;
                    candidate = handler;
                }
            }
        }
        return candidate;
    }
}
