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

package com.hazelcast.core;

/**
 * Type of entry event.
 */
public enum EntryEventType {

    ADDED(1),
    REMOVED(2),
    UPDATED(3),
    EVICTED(4),
    EVICT_ALL(5),
    CLEAR_ALL(6),
    MERGED(7);

    private int type;

    private EntryEventType(final int type) {
        this.type = type;
    }

    /**
     * Returns the event type.
     *
     * @return the event type.
     */
    public int getType() {
        return type;
    }

    /**
     * Returns the EntryEventType as an enum.
     *
     * @return the EntryEventType as an enum.
     */
    public static EntryEventType getByType(final int eventType) {
        for (EntryEventType entryEventType : values()) {
            if (entryEventType.type == eventType) {
                return entryEventType;
            }
        }
        return null;
    }
}
