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
import com.hazelcast.internal.management.events.EventMetadata.EventType;
import com.hazelcast.internal.util.UuidUtil;

import static com.hazelcast.internal.management.events.EventMetadata.EventType.ADD_WAN_CONFIGURATION_IGNORED;

public final class WanAddConfigurationIgnoredEvent extends AbstractWanConfigurationEventBase {
    private final String reason;

    private WanAddConfigurationIgnoredEvent(String wanConfigName, String reason) {
        super(UuidUtil.newUnsecureUUID(), wanConfigName);
        this.reason = reason;
    }

    public static WanAddConfigurationIgnoredEvent alreadyExists(String wanConfigName) {
        return new WanAddConfigurationIgnoredEvent(wanConfigName,
                "A WAN replication config already exists with the given name.");
    }

    public static WanAddConfigurationIgnoredEvent enterpriseOnly(String wanConfigName) {
        return new WanAddConfigurationIgnoredEvent(wanConfigName,
                "Adding new WAN replication config is supported for enterprise clusters only.");
    }

    @Override
    public EventType getType() {
        return ADD_WAN_CONFIGURATION_IGNORED;
    }

    @Override
    public JsonObject toJson() {
        JsonObject json = super.toJson();
        json.add("reason", reason);
        return json;
    }
}
