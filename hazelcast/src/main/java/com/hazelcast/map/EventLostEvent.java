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

import com.hazelcast.core.EntryEventType;
import com.hazelcast.cluster.Member;

/**
 * This event is fired in case of an event lost detection.
 * The fired event can be caught by implementing {@link com.hazelcast.map.listener.EventLostListener EventLostListener}.
 *
 * @see com.hazelcast.map.listener.EventLostListener
 */
public class EventLostEvent implements IMapEvent {

    /**
     * Event type ID.
     *
     * @see EntryEventType
     */
    public static final int EVENT_TYPE = getNextEntryEventTypeId();

    // TODO EntryEvenType extensibility.
    private final int partitionId;

    private final String source;

    private final Member member;

    public EventLostEvent(String source, Member member, int partitionId) {
        this.source = source;
        this.member = member;
        this.partitionId = partitionId;
    }

    public int getPartitionId() {
        return partitionId;
    }

    @Override
    public Member getMember() {
        return member;
    }

    /**
     * Intentionally returns {@code null}.
     *
     * @return {@code null}
     */
    @Override
    public EntryEventType getEventType() {
        return null;
    }

    @Override
    public String getName() {
        return source;
    }

    /**
     * Returns next event type ID.
     *
     * @return next event type ID
     * @see EntryEventType
     */
    private static int getNextEntryEventTypeId() {
        int higherTypeId = Integer.MIN_VALUE;
        int i = 0;
        EntryEventType[] values = EntryEventType.values();
        for (EntryEventType value : values) {
            int typeId = value.getType();
            if (i == 0) {
                higherTypeId = typeId;
            } else {
                if (typeId > higherTypeId) {
                    higherTypeId = typeId;
                }
            }
            i++;
        }

        int eventFlagPosition = Integer.numberOfTrailingZeros(higherTypeId);

        return 1 << ++eventFlagPosition;
    }

    @Override
    public String toString() {
        return "EventLostEvent{"
                + "partitionId=" + partitionId
                + ", source='" + source + '\''
                + ", member=" + member
                + '}';
    }
}
