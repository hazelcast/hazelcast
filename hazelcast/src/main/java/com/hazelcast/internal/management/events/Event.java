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

/**
 * Represents events sent to Management Center.
 * <p>
 * Events are sent to Management Center periodically, which then informs the user about them.
 * Events can be used where a log statement normally is used. They are usually generated as
 * a result of an action that is triggered by the Management Center.
 *
 * @see com.hazelcast.internal.management.ManagementCenterService#log(Event)
 * @since 3.11
 */
public interface Event {
    EventType getType();

    /**
     * Returns when the event happened as epoch millis.
     */
    long getTimestamp();

    /**
     * Serialize the event as JSON, for sending to Management Center.
     */
    JsonObject toJson();
}
