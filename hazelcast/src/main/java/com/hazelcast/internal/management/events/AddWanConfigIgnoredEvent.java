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

import static com.hazelcast.internal.management.events.EventMetadata.EventType.ADD_WAN_CONFIGURATION_IGNORED;

public final class AddWanConfigIgnoredEvent extends AbstractEventBase {
    private final String wanConfigName;
    private final String reason;

    private AddWanConfigIgnoredEvent(String wanConfigName, String reason) {
        this.wanConfigName = wanConfigName;
        this.reason = reason;
    }

    public static AddWanConfigIgnoredEvent alreadyExists(String wanConfigName) {
        return new AddWanConfigIgnoredEvent(wanConfigName,
                "A WAN replication config already exists with the given name.");
    }

    public static AddWanConfigIgnoredEvent enterpriseOnly(String wanConfigName) {
        return new AddWanConfigIgnoredEvent(wanConfigName,
                "Adding new WAN replication config is supported for enterprise clusters only.");
    }

    @Override
    public EventType getType() {
        return ADD_WAN_CONFIGURATION_IGNORED;
    }

    @Override
    public JsonObject toJson() {
        JsonObject json = new JsonObject();
        json.add("wanConfigName", wanConfigName);
        json.add("reason", reason);
        return json;
    }
}
