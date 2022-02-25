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

/**
 * Used for map-wide events like {@link com.hazelcast.core.EntryEventType#EVICT_ALL}
 * and {@link  com.hazelcast.core.EntryEventType#CLEAR_ALL}.
 *
 * @see com.hazelcast.map.listener.MapListener
 * @see com.hazelcast.core.EntryListener
 */
public class MapEvent extends AbstractIMapEvent {

    private static final long serialVersionUID = -4948640313865667023L;

    /**
     * Number of entries affected by this event.
     */
    private final int numberOfEntriesAffected;

    public MapEvent(Object source, Member member, int eventType, int numberOfEntriesAffected) {
        super(source, member, eventType);
        this.numberOfEntriesAffected = numberOfEntriesAffected;
    }

    /**
     * Returns the number of entries affected by this event.
     *
     * @return number of entries affected by this event
     */
    public int getNumberOfEntriesAffected() {
        return numberOfEntriesAffected;
    }


    @Override
    public String toString() {
        return "MapEvent{"
                + super.toString()
                + ", numberOfEntriesAffected=" + numberOfEntriesAffected
                + '}';

    }
}
