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

package com.hazelcast.config;

/**
 * Determines the behavior of WAN replication
 * impl. In case of WAN event queues are full.
 */
public enum WanQueueFullBehavior {

    /**
     * Instruct WAN replication implementation to
     * drop new events when WAN event queues are full.
     */
    DISCARD_AFTER_MUTATION(0),

    /**
     * Instruct WAN replication implementation to throw
     * an exception and doesn't allow further processing.
     */
    THROW_EXCEPTION(1),

    /**
     * Similar to {@link #THROW_EXCEPTION} but only throws
     * exception when WAN replication is active. Discards
     * the new events if WAN replication is stopped.
     */
    THROW_EXCEPTION_ONLY_IF_REPLICATION_ACTIVE(2);

    private final int id;

    WanQueueFullBehavior(int id) {
        this.id = id;
    }

    /**
     * Gets the ID for the given {@link WanQueueFullBehavior}.
     * <p>
     * The reason this ID is used instead of an the ordinal value is that the ordinal value is more prone to changes due to
     * reordering.
     *
     * @return the ID
     */
    public int getId() {
        return id;
    }

    /**
     * Returns the WANQueueFullBehavior as an enum.
     *
     * @return the WANQueueFullBehavior as an enum
     */
    public static WanQueueFullBehavior getByType(final int id) {
        for (WanQueueFullBehavior behavior : values()) {
            if (behavior.id == id) {
                return behavior;
            }
        }
        return null;
    }
}
