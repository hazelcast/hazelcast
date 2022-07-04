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
import com.hazelcast.internal.management.events.EventMetadata.EventType;
import com.hazelcast.internal.util.UuidUtil;

import java.util.Collection;

import static com.hazelcast.internal.management.events.EventMetadata.EventType.WAN_CONFIGURATION_EXTENDED;

public class WanConfigurationExtendedEvent extends AbstractWanConfigurationEventBase {
    private final Collection<String> wanPublisherIds;

    public WanConfigurationExtendedEvent(String wanConfigName, Collection<String> wanPublisherIds) {
        super(UuidUtil.newUnsecureUUID(), wanConfigName);
        this.wanPublisherIds = wanPublisherIds;
    }

    @Override
    public EventType getType() {
        return WAN_CONFIGURATION_EXTENDED;
    }

    @Override
    public JsonObject toJson() {
        JsonObject json = super.toJson();
        JsonArray publisherIds = new JsonArray();
        for (String publisherId : wanPublisherIds) {
            publisherIds.add(publisherId);
        }
        json.add("wanPublisherIds", publisherIds);
        return json;
    }
}
