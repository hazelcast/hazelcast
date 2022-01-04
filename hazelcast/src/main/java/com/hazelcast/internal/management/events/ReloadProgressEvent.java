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

import com.hazelcast.internal.json.JsonObject;


public class ReloadProgressEvent extends AbstractEventBase implements ReloadEvent {

    // total number of dynamic changes to be applied to this cluster during this reload
    private final int numChanges;

    // rank of the dynamic change of this event (for example 2 for second change)
    private final int currentChange;

    public ReloadProgressEvent(int numChanges, int currentChange) {
        this.numChanges = numChanges;
        this.currentChange = currentChange;
    }

    @Override
    public EventMetadata.EventType getType() {
        return EventMetadata.EventType.CONFIG_RELOAD_PROGRESS;
    }

    @Override
    public JsonObject toJson() {
        JsonObject json = new JsonObject();
        json.add("numChanges", numChanges);
        json.add("currentChange", currentChange);
        return json;
    }

    public int getNumChanges() {
        return numChanges;
    }

    public int getCurrentChange() {
        return currentChange;
    }
}
