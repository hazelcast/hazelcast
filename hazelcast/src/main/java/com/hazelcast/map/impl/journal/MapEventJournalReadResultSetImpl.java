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

import com.hazelcast.projection.Projection;
import com.hazelcast.ringbuffer.impl.ReadResultSetImpl;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.function.Predicate;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class MapEventJournalReadResultSetImpl<K, V, T> extends ReadResultSetImpl<InternalEventJournalMapEvent, T> {

    MapEventJournalReadResultSetImpl(int minSize, int maxSize, SerializationService serializationService,
                                     final Predicate<? super EventJournalMapEvent<K, V>> predicate,
                                     final Projection<? super EventJournalMapEvent<K, V>, T> projection) {
        super(minSize, maxSize, serializationService,
                predicate == null ? null : new Predicate<InternalEventJournalMapEvent>() {
                    @Override
                    @SuppressWarnings("unchecked")
                    @SuppressFBWarnings("BC_UNCONFIRMED_CAST")
                    public boolean test(InternalEventJournalMapEvent e) {
                        return predicate.test((DeserialisingEventJournalMapEvent<K, V>) e);
                    }
                },
                projection == null ? null : new Projection<InternalEventJournalMapEvent, T>() {
                    @Override
                    @SuppressWarnings("unchecked")
                    @SuppressFBWarnings("BC_UNCONFIRMED_CAST")
                    public T transform(InternalEventJournalMapEvent input) {
                        return projection.transform((DeserialisingEventJournalMapEvent<K, V>) input);
                    }
                });
    }

    @Override
    public void addItem(long seq, Object item) {
        final InternalEventJournalMapEvent e = serializationService.toObject(item);
        final DeserialisingEventJournalMapEvent<K, V> deserialisingEvent
                = new DeserialisingEventJournalMapEvent<K, V>(serializationService, e);
        super.addItem(seq, deserialisingEvent);
    }

}
