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
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.services.ObjectNamespace;

import java.util.UUID;

public class ConfigReloadFailedEvent extends AbstractIdentifiedEvent {
    private final Exception exception;
    private final ObjectNamespace namespace;

    public ConfigReloadFailedEvent(UUID uuid, Exception exception, ObjectNamespace namespace) {
        super(uuid);
        this.exception = exception;
        this.namespace = namespace;
    }

    @Override
    public EventMetadata.EventType getType() {
        return EventMetadata.EventType.CONFIG_RELOAD_FAILED;
    }

    @Override
    public JsonObject toJson() {
        JsonObject json = super.toJson();
        json.add("exception", exception.getClass().getSimpleName());
        json.add("exceptionMessage", exception.getMessage());
        if (namespace != null) {
            json.add("configName", namespace.getObjectName());
            json.add("sectionName", ConfigSections.Translate.toSectionName(namespace.getServiceName()));
        }
        return json;
    }

    public Exception getException() {
        return exception;
    }

    public ObjectNamespace getNamespace() {
        return namespace;
    }

}
