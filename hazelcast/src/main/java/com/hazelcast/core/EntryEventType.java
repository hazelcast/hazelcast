/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

    ADDED(Type.ADDED),
    REMOVED(Type.REMOVED),
    UPDATED(Type.UPDATED),
    EVICTED(Type.EVICTED),
    EVICT_ALL(Type.EVICT_ALL),
    CLEAR_ALL(Type.CLEAR_ALL),
    MERGED(Type.MERGED),
    EXPIRED(Type.EXPIRED),
    INVALIDATION(Type.INVALIDATION);

    private int type;

    EntryEventType(final int type) {
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

    /**
     * These constants represent type-id and bit-mask of events.
     *
     * @see com.hazelcast.map.impl.MapListenerFlagOperator
     */
    //CHECKSTYLE:OFF
    private static class Type {
        private static final int ADDED = 1;
        private static final int REMOVED = 1 << 1;
        private static final int UPDATED = 1 << 2;
        private static final int EVICTED = 1 << 3;
        private static final int EVICT_ALL = 1 << 4;
        private static final int CLEAR_ALL = 1 << 5;
        private static final int MERGED = 1 << 6;
        private static final int EXPIRED = 1 << 7;
        private static final int INVALIDATION = 1 << 8;
    }
    //CHECKSTYLE:ON
}
