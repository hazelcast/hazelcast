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

package com.hazelcast.cache.impl.event;

import com.hazelcast.cache.CacheEventType;
import com.hazelcast.cluster.Member;

/**
 * Cache events common contract.
 * @since 3.6
 */
public interface ICacheEvent {

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
    CacheEventType getEventType();

    /**
     * Returns the name of the cache for this event.
     *
     * @return name of the cache for this event.
     */
    String getName();

}
