/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.diagnostics.DiagnosticsConfig;
import com.hazelcast.internal.json.JsonObject;

import java.util.UUID;

public class DiagnosticsConfigUpdatedEvent extends AbstractEventBase {

    protected DiagnosticsConfig diagnosticsConfig;
    protected UUID memberId;

    public DiagnosticsConfigUpdatedEvent(DiagnosticsConfig diagnosticsConfig, UUID memberId) {
        this.diagnosticsConfig = diagnosticsConfig;
        this.memberId = memberId;
    }

    @Override
    public EventMetadata.EventType getType() {
        return EventMetadata.EventType.DIAGNOSTICS_CONFIG_UPDATED;
    }

    @Override
    public JsonObject toJson() {
        JsonObject json = new JsonObject();

        json.add("enabled", diagnosticsConfig.isEnabled());
        json.add("max-rolled-file-size-in-mb", diagnosticsConfig.getMaxRolledFileSizeInMB());
        json.add("max-rolled-file-count", diagnosticsConfig.getMaxRolledFileCount());
        json.add("include-epoch-time", diagnosticsConfig.isIncludeEpochTime());
        json.add("log-directory", diagnosticsConfig.getLogDirectory());
        json.add("file-name-prefix", diagnosticsConfig.getFileNamePrefix());
        json.add("output-type", diagnosticsConfig.getOutputType().name());
        json.add("auto-off-timer-in-minutes", diagnosticsConfig.getAutoOffDurationInMinutes());
        json.add("member-id", memberId.toString());

        JsonObject properties = new JsonObject();
        for (String key : diagnosticsConfig.getPluginProperties().keySet()) {
            properties.add(key, diagnosticsConfig.getPluginProperties().get(key));
        }
        json.add("plugin-properties", properties);

        return json;
    }
}
