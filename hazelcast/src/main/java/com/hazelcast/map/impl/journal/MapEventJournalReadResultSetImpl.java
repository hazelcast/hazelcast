/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.journal;

import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.journal.EventJournalMapEvent;
import com.hazelcast.nio.serialization.SerializableByConvention;
import com.hazelcast.projection.Projection;
import com.hazelcast.ringbuffer.impl.ReadResultSetImpl;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.function.Predicate;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class MapEventJournalReadResultSetImpl<K, V, T> extends ReadResultSetImpl<InternalEventJournalMapEvent, T> {

    public MapEventJournalReadResultSetImpl() {
    }

    MapEventJournalReadResultSetImpl(int minSize, int maxSize, SerializationService serializationService,
                                     final Predicate<? super EventJournalMapEvent<K, V>> predicate,
                                     final Projection<? super EventJournalMapEvent<K, V>, T> projection) {
        super(minSize, maxSize, serializationService,
                predicate == null ? null : new Predicate<InternalEventJournalMapEvent>() {
                    @Override
                    @SuppressWarnings("unchecked")
                    @SuppressFBWarnings("BC_UNCONFIRMED_CAST")
                    public boolean test(InternalEventJournalMapEvent e) {
                        return predicate.test((DeserializingEventJournalMapEvent<K, V>) e);
                    }
                },
                projection == null ? null : new ProjectionAdapter<K, V, T>(projection));
    }

    @Override
    public void addItem(long seq, Object item) {
        // the event journal ringbuffer supports only OBJECT format for now
        final InternalEventJournalMapEvent e = (InternalEventJournalMapEvent) item;
        final DeserializingEventJournalMapEvent<K, V> deserialisingEvent
                = new DeserializingEventJournalMapEvent<K, V>(serializationService, e);
        super.addItem(seq, deserialisingEvent);
    }

    @Override
    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return MapDataSerializerHook.EVENT_JOURNAL_READ_RESULT_SET;
    }

    @SerializableByConvention
    private static class ProjectionAdapter<K, V, T> extends Projection<InternalEventJournalMapEvent, T> {

        private final Projection<? super EventJournalMapEvent<K, V>, T> projection;

        ProjectionAdapter(Projection<? super EventJournalMapEvent<K, V>, T> projection) {
            this.projection = projection;
        }

        @Override
        @SuppressWarnings("unchecked")
        @SuppressFBWarnings("BC_UNCONFIRMED_CAST")
        public T transform(InternalEventJournalMapEvent e) {
            return projection.transform((DeserializingEventJournalMapEvent<K, V>) e);
        }
    }
}
