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
import java.util.Set;

/**
 * Simple Migration Strategy for Handlers.
 *
 * It attempts to trigger a migration if a ratio between least busy and most busy selectors
 * exceeds {@link #MIN_MAX_RATIO_MIGRATION_THRESHOLD}.
 *
 * Once a migration is triggered it tries to find the busiest handler registered in
 * {@link com.hazelcast.nio.tcp.handlermigration.BalancerState#sourceSelector} which wouldn't cause
 * overload of the {@link com.hazelcast.nio.tcp.handlermigration.BalancerState#destinationSelector} after a migration.
 *
 */
class MigrationStrategy {

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
     * @param state
     * @return <code>true</code> if imbalance threshold has been reached and migration should be attempted
     */
    boolean shouldAttemptToScheduleMigration(BalancerState state) {
        long min = state.minimumEvents;
        long max = state.maximumEvents;

        if (min == Long.MIN_VALUE || max == Long.MAX_VALUE) {
            return false;
        }
        long lowerBound = (long) (MIN_MAX_RATIO_MIGRATION_THRESHOLD * max);
        return min < lowerBound;
    }

    /**
     * Attempt to find a handler to migrate to a new IOSelector.
     *
     * @param state describing a snapshot of IOSelector load
     * @return a handler to migrate to a new IOSelector or null if no suitable handler is found.
     */
     MigratableHandler findHandlerToMigrate(BalancerState state) {
        Set<? extends MigratableHandler> candidates = state.getHandlersOwnerBy(state.sourceSelector);
        long migrationThreshold = (long) ((state.maximumEvents - state.minimumEvents)
                * MAXIMUM_NO_OF_EVENTS_AFTER_MIGRATION_COEFFICIENT);
        MigratableHandler candidate = null;
        long noOfEventsInSelectedHandler = 0;
        for (MigratableHandler handler : candidates) {
            long noOfEvents = state.getNoOfEvents(handler);
            if (noOfEvents > noOfEventsInSelectedHandler) {
                if (noOfEvents < migrationThreshold) {
                    noOfEventsInSelectedHandler = noOfEvents;
                    candidate = handler;
                }
            }
        }
        return candidate;
    }

}
