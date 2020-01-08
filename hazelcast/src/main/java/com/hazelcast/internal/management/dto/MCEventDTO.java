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

package com.hazelcast.internal.management.dto;

import com.hazelcast.internal.management.events.Event;

/**
 * A DTO that describes event sent to Management Center.
 *
 * @see com.hazelcast.internal.management.events.Event
 */
public final class MCEventDTO {

    private final long timestamp;
    private final int type;
    private final String dataJson;

    public MCEventDTO(long timestamp, int type, String dataJson) {
        this.timestamp = timestamp;
        this.type = type;
        this.dataJson = dataJson;
    }

    public static MCEventDTO fromEvent(Event event) {
        return new MCEventDTO(event.getTimestamp(), event.getType().getCode(), event.toJson().toString());
    }

    public long getTimestamp() {
        return timestamp;
    }

    public int getType() {
        return type;
    }

    public String getDataJson() {
        return dataJson;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MCEventDTO eventDTO = (MCEventDTO) o;

        if (timestamp != eventDTO.timestamp) {
            return false;
        }
        if (type != eventDTO.type) {
            return false;
        }
        return dataJson.equals(eventDTO.dataJson);
    }

    @Override
    public int hashCode() {
        int result = (int) (timestamp ^ (timestamp >>> 32));
        result = 31 * result + type;
        result = 31 * result + dataJson.hashCode();
        return result;
    }

}
