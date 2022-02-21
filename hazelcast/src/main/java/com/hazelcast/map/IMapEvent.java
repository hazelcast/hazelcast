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

package com.hazelcast.map;

import com.hazelcast.cluster.Member;
import com.hazelcast.core.EntryEventType;

/**
 * Map events common contract.
 */
public interface IMapEvent {

    /**
     * Returns the member that fired this event.
     *
     * @return the member that fired this event.
     */
    Member getMember();

    /**
     * Return the event type
     *
     * @return event type
     */
    EntryEventType getEventType();

    /**
     * Returns the name of the map for this event.
     *
     * @return name of the map for this event.
     */
    String getName();

}
