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

package com.hazelcast.internal.management.events;

import com.hazelcast.internal.json.JsonObject;

import java.util.Map;

import static java.util.Arrays.stream;
import static java.util.stream.Collectors.toMap;

public final class EventMetadata {
    private final EventType type;
    private final long timestamp;

    public EventMetadata(EventType type, long timestamp) {
        this.type = type;
        this.timestamp = timestamp;
    }

    // WARNING: Used by Management Center. Do not rename any enum, change its code
    // or remove an existing enum, only add new ones.
    public enum EventType {
        WAN_CONSISTENCY_CHECK_STARTED(1),
        WAN_CONSISTENCY_CHECK_FINISHED(2),
        WAN_SYNC_STARTED(3),
        WAN_SYNC_FINISHED_FULL(4),
        WAN_CONSISTENCY_CHECK_IGNORED(5),
        WAN_SYNC_PROGRESS_UPDATE(6),
        WAN_SYNC_FINISHED_MERKLE(7),
        WAN_CONFIGURATION_ADDED(8),
        ADD_WAN_CONFIGURATION_IGNORED(9),
        WAN_SYNC_IGNORED(10),
        WAN_CONFIGURATION_EXTENDED(11),
        CONFIG_UPDATE_STARTED(12),
        CONFIG_UPDATE_PROGRESS(13),
        CONFIG_UPDATE_FINISHED(14),
        CONFIG_UPDATE_FAILED(15);

        private static final Map<Integer, EventType> CODE_MAPPING = stream(EventType.values())
                .collect(toMap(EventType::getCode, eventType -> eventType));

        private final int code;

        EventType(int code) {
            this.code = code;
        }

        public int getCode() {
            return code;
        }

        @SuppressWarnings("unused")
        // used by Management Center
        public static EventType withCode(int code) {
            EventType type = CODE_MAPPING.get(code);
            if (type == null) {
                throw new IllegalArgumentException("No EventType with code " + code);
            }
            return type;
        }

        @Override
        public String toString() {
            return "EventType{"
                    + "name=" + name()
                    + ", code=" + code
                    + '}';
        }
    }

    public JsonObject toJson() {
        JsonObject json = new JsonObject();
        json.add("type", type.code);
        json.add("timestamp", timestamp);
        return json;
    }
}
