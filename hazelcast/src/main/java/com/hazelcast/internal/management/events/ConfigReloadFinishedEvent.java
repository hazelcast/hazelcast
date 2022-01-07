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

import com.hazelcast.internal.config.dynamic.reload.ReloadResult;
import com.hazelcast.internal.json.JsonObject;

import java.util.UUID;

public class ConfigReloadFinishedEvent extends AbstractIdentifiedEvent {

    private final ReloadResult reloadResult;

    public ConfigReloadFinishedEvent(UUID uuid, ReloadResult reloadResult) {
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
        json.add("reloadResult", reloadResult.toJson());
        return json;
    }

    public ReloadResult getReloadResult() {
        return reloadResult;
    }
}

