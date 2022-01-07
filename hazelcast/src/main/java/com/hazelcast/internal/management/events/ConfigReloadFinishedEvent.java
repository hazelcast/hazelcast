/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.config.ConfigSections;
import com.hazelcast.internal.json.JsonArray;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.services.ObjectNamespace;

import java.util.Set;
import java.util.UUID;

public class ConfigReloadFinishedEvent extends AbstractIdentifiedEvent {

    private final Set<ObjectNamespace> reloadResult;

    public ConfigReloadFinishedEvent(UUID uuid, Set<ObjectNamespace> reloadResult) {
        super(uuid);
        this.reloadResult = reloadResult;
    }

    @Override
    public EventMetadata.EventType getType() {
        return EventMetadata.EventType.CONFIG_RELOAD_FINISHED;
    }

    @Override
    public JsonObject toJson() {
        JsonObject json = super.toJson();

        JsonArray results = new JsonArray();
        for (ObjectNamespace ns : reloadResult) {
            JsonObject namespaceAsJson = new JsonObject();
            namespaceAsJson.add("sectionName", ConfigSections.Translate.toSectionName(ns.getServiceName()));
            namespaceAsJson.add("objectName", ns.getObjectName());
            results.add(namespaceAsJson);
        }

        json.add("reloadResult", results);
        return json;
    }

    public Set<ObjectNamespace> getReloadResult() {
        return reloadResult;
    }
}

