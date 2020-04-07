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

package com.hazelcast.internal.management.events;

import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.management.events.EventMetadata.EventType;

import static com.hazelcast.internal.management.events.EventMetadata.EventType.WAN_CONFIGURATION_ADDED;

public class WanConfigurationAddedEvent extends AbstractEventBase {
    private final String wanConfigName;

    public WanConfigurationAddedEvent(String wanConfigName) {
        this.wanConfigName = wanConfigName;
    }

    @Override
    public EventType getType() {
        return WAN_CONFIGURATION_ADDED;
    }

    @Override
    public JsonObject toJson() {
        JsonObject json = new JsonObject();
        json.add("wanConfigName", wanConfigName);
        return json;
    }
}
