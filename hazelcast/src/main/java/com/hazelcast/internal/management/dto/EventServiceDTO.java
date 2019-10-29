/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.json.JsonSerializable;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.spi.impl.eventservice.EventService;

import static com.hazelcast.internal.util.JsonUtil.getInt;

/**
 * A Serializable DTO for {@link com.hazelcast.internal.jmx.EventServiceMBean}.
 */
public class EventServiceDTO implements JsonSerializable {

    public int eventThreadCount;
    public int eventQueueCapacity;
    public int eventQueueSize;

    public EventServiceDTO() {
    }

    public EventServiceDTO(EventService es) {
        this.eventThreadCount = es.getEventThreadCount();
        this.eventQueueCapacity = es.getEventQueueCapacity();
        this.eventQueueSize = es.getEventQueueSize();
    }

    @Override
    public JsonObject toJson() {
        final JsonObject root = new JsonObject();
        root.add("eventThreadCount", eventThreadCount);
        root.add("eventQueueCapacity", eventQueueCapacity);
        root.add("eventQueueSize", eventQueueSize);
        return root;
    }

    @Override
    public void fromJson(JsonObject json) {
        eventThreadCount = getInt(json, "eventThreadCount", -1);
        eventQueueCapacity = getInt(json, "eventQueueCapacity", -1);
        eventQueueSize = getInt(json, "eventQueueSize", -1);

    }
}
