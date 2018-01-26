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

    ADDED(TypeId.ADDED),
    REMOVED(TypeId.REMOVED),
    UPDATED(TypeId.UPDATED),
    EVICTED(TypeId.EVICTED),
    EVICT_ALL(TypeId.EVICT_ALL),
    CLEAR_ALL(TypeId.CLEAR_ALL),
    MERGED(TypeId.MERGED),
    EXPIRED(TypeId.EXPIRED),
    INVALIDATION(TypeId.INVALIDATION);

    private int typeId;

    EntryEventType(final int typeId) {
        this.typeId = typeId;
    }

    /**
     * @return the event type ID.
     */
    public int getType() {
        return typeId;
    }

    /**
     * @return the matching EntryEventType for the supplied {@code typeId} or null if there is no match.
     */
    @SuppressWarnings("checkstyle:returncount")
    public static EntryEventType getByType(final int typeId) {
        switch (typeId) {
            case TypeId.ADDED:
                return ADDED;
            case TypeId.REMOVED:
                return REMOVED;
            case TypeId.UPDATED:
                return UPDATED;
            case TypeId.EVICTED:
                return EVICTED;
            case TypeId.EVICT_ALL:
                return EVICT_ALL;
            case TypeId.CLEAR_ALL:
                return CLEAR_ALL;
            case TypeId.MERGED:
                return MERGED;
            case TypeId.EXPIRED:
                return EXPIRED;
            case TypeId.INVALIDATION:
                return INVALIDATION;
            default:
                return null;
        }
    }

    /**
     * These constants represent event type ID and bit-mask of events.
     *
     * @see com.hazelcast.map.impl.MapListenerFlagOperator
     */
    //CHECKSTYLE:OFF
    private static class TypeId {
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
