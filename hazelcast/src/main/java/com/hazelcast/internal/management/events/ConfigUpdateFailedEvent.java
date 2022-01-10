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

import com.hazelcast.internal.config.ConfigNamespace;
import com.hazelcast.internal.dynamicconfig.ConfigUpdateResult;
import com.hazelcast.internal.json.JsonObject;

import java.util.UUID;

public class ConfigUpdateFailedEvent extends AbstractIdentifiedEvent {
    private final Exception exception;
    private final ConfigNamespace namespace;
    private final ConfigUpdateResult configUpdateResult;

    public ConfigUpdateFailedEvent(UUID uuid, Exception exception,
                                   ConfigNamespace namespace,
                                   ConfigUpdateResult configUpdateResult) {
        super(uuid);
        this.exception = exception;
        this.namespace = namespace;
        this.configUpdateResult = configUpdateResult;
    }

    @Override
    public EventMetadata.EventType getType() {
        return EventMetadata.EventType.CONFIG_UPDATE_FAILED;
    }

    @Override
    public JsonObject toJson() {
        JsonObject json = super.toJson();
        json.add("exception", exception.getClass().getSimpleName());
        json.add("exceptionMessage", exception.getMessage());
        json.add("configUpdateResult", configUpdateResult.toJson());
        if (namespace != null) {
            json.add("configName", namespace.getConfigName() != null ? namespace.getConfigName() : "null");
            json.add("sectionName", namespace.getSectionName());
        }
        return json;
    }

    public Exception getException() {
        return exception;
    }

    public ConfigNamespace getNamespace() {
        return namespace;
    }

}
