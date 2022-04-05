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

import com.hazelcast.internal.config.ConfigNamespace;
import com.hazelcast.internal.json.JsonObject;

import java.util.UUID;

public class ConfigUpdateProgressEvent extends AbstractConfigUpdateEvent {

    // total number of dynamic changes to be applied to this cluster during this reload
    private final int totalChangeCount;

    // rank of the dynamic change of this event (for example 2 for second change)
    private final int appliedChangeCount;

    private final ConfigNamespace namespace;

    public ConfigUpdateProgressEvent(UUID configUpdateProcessId, int totalChangeCount, int appliedChangeCount,
                                     ConfigNamespace namespace) {
        super(configUpdateProcessId);
        this.totalChangeCount = totalChangeCount;
        this.appliedChangeCount = appliedChangeCount;
        this.namespace = namespace;
    }

    @Override
    public EventMetadata.EventType getType() {
        return EventMetadata.EventType.CONFIG_UPDATE_PROGRESS;
    }

    @Override
    public JsonObject toJson() {
        JsonObject json = super.toJson();
        json.add("totalChangeCount", totalChangeCount);
        json.add("appliedChangeCount", appliedChangeCount);
        json.add("configName", namespace.getConfigName());
        json.add("sectionName", namespace.getSectionName());
        return json;
    }

    public int getTotalChangeCount() {
        return totalChangeCount;
    }

    public int getAppliedChangeCount() {
        return appliedChangeCount;
    }

    public ConfigNamespace getNamespace() {
        return namespace;
    }
}
