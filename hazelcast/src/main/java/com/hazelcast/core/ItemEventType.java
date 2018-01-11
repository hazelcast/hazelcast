/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
 * Type of item event.
 */
public enum ItemEventType {
    ADDED(1),
    REMOVED(2);

    private int type;

    private ItemEventType(final int type) {
        this.type = type;
    }

    public int getType() {
        return type;
    }

    public static ItemEventType getByType(final int eventType) {
        for (ItemEventType entryEventType : values()) {
            if (entryEventType.type == eventType) {
                return entryEventType;
            }
        }
        return null;
    }
}
