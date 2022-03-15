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

package com.hazelcast.wan.impl;

/**
 * WAN sync types. These define the scope of entries which need to be synced.
 */
public enum WanSyncType {

    /**
     * Sync all entries for all maps.
     */
    ALL_MAPS(0),

    /**
     * Sync all entries for a specific map.
     */
    SINGLE_MAP(1);

    private int type;

    WanSyncType(final int type) {
        this.type = type;
    }

    /**
     * @return unique ID of the event type
     */
    public int getType() {
        return type;
    }

    public static WanSyncType getByType(final int eventType) {
        for (WanSyncType entryEventType : values()) {
            if (entryEventType.type == eventType) {
                return entryEventType;
            }
        }
        return null;
    }
}
