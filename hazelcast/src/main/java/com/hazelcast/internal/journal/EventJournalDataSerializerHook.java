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

package com.hazelcast.internal.journal;

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.EVENT_JOURNAL_DS_FACTORY;
import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.EVENT_JOURNAL_DS_FACTORY_ID;

/**
 * Data serializer hook for common classes related to the {@link EventJournal}. Data structure specific
 * (map, cache,...) event journal classes are under the other respective serializer hooks.
 */
public final class EventJournalDataSerializerHook implements DataSerializerHook {
    /**
     * Factory ID for the event journal {@link IdentifiedDataSerializable} classes
     */
    public static final int F_ID = FactoryIdHelper.getFactoryId(EVENT_JOURNAL_DS_FACTORY, EVENT_JOURNAL_DS_FACTORY_ID);

    /**
     * Type ID for the {@link EventJournalInitialSubscriberState} class
     */
    public static final int EVENT_JOURNAL_INITIAL_SUBSCRIBER_STATE = 1;
    public static final int DESERIALIZING_ENTRY = 2;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        return new DataSerializableFactory() {
            @Override
            public IdentifiedDataSerializable create(int typeId) {
                switch (typeId) {
                    case EVENT_JOURNAL_INITIAL_SUBSCRIBER_STATE:
                        return new EventJournalInitialSubscriberState();
                    case DESERIALIZING_ENTRY:
                        return new DeserializingEntry();
                    default:
                        return null;
                }
            }
        };
    }
}
