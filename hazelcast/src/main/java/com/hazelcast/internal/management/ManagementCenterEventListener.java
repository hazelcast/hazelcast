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

package com.hazelcast.internal.management;

import com.hazelcast.internal.management.events.Event;

/**
 * Callback interface that captures {@link Event}s logged to Management
 * Center. It captures only events that are about to be sent to Management
 * Center. If the {@link ManagementCenterService} is not running or not
 * enabled, this event listener is not called.
 *
 * @see ManagementCenterService#log(Event)
 */
@FunctionalInterface
public interface ManagementCenterEventListener {

    /**
     * Callback for logging MC events.
     *
     * @param event The event logged
     */
    void onEventLogged(Event event);
}
