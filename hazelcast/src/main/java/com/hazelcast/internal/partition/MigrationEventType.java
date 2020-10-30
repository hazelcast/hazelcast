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

package com.hazelcast.internal.partition;


/**
 * Migration event types.
 * @see MigrationEventHandler
 */
public enum MigrationEventType {

    /**
     * An event type indicating that the migration process started.
     */
    MIGRATION_STARTED(0),

    /**
     * An event type indicating that the migration process finished.
     */
    MIGRATION_FINISHED(1);

    private final int type;

    MigrationEventType(final int type) {
        this.type = type;
    }

    /**
     * @return unique ID of the event type.
     */
    public int getType() {
        return type;
    }

    public static MigrationEventType getByType(final int type) {
        if (type == MIGRATION_STARTED.type) {
            return MIGRATION_STARTED;
        }
        if (type == MIGRATION_FINISHED.type) {
            return MIGRATION_FINISHED;
        }
        throw new IllegalArgumentException("Invalid event type: " + type);
    }
}
