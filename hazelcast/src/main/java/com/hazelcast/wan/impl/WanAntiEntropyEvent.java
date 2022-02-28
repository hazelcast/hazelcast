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

package com.hazelcast.wan.impl;

import java.util.Set;
import java.util.UUID;

/**
 * A user-triggered WAN anti-entropy event. The concrete event may be of
 * different types, e.g. consistency check or synchronization event.
 */
public interface WanAntiEntropyEvent {
    /**
     * Returns the source cluster-wide unique ID for this anti-entropy event.
     *
     * @return the unique ID for this event
     */
    UUID getUuid();

    /**
     * Returns the distributed structure (object) name on which this event
     * occurred. It may be {@code null} if the event is for all structures
     * (objects) for a certain distributed service.
     *
     * @return the object name on which this event occurred
     */
    String getObjectName();

    /**
     * Returns the set of partition IDs on which this event occurred.
     *
     * @return the set of partition IDs
     */
    Set<Integer> getPartitionSet();
}
