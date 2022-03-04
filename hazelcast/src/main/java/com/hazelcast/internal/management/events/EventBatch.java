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

import com.hazelcast.internal.json.JsonArray;
import com.hazelcast.internal.json.JsonObject;

import java.util.List;

/**
 * Batch of events to be sent to Management Center.
 * <p>
 * Contains the cluster name, member address and list of events ordered by their occurrence time.
 * Serialized as JSON for sending to Management Center.
 */
public class EventBatch {
    private final String cluster;
    private final String address;
    private final List<Event> events;

    public EventBatch(String cluster, String address, List<Event> events) {
        this.cluster = cluster;
        this.address = address;
        this.events = events;
    }

    public JsonObject toJson() {
        JsonObject json = new JsonObject();
        json.add("cluster", cluster);
        json.add("address", address);
        JsonArray eventJsonArray = new JsonArray();
        for (Event event : events) {
            JsonObject metadataJson = new JsonObject();
            metadataJson.add("type", event.getType().getCode());
            metadataJson.add("timestamp", event.getTimestamp());

            JsonObject eventJson = new JsonObject();
            eventJson.add("metadata", metadataJson);
            eventJson.add("data", event.toJson());

            eventJsonArray.add(eventJson);
        }
        json.add("events", eventJsonArray);
        return json;
    }
}
