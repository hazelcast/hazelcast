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

package com.hazelcast.cache.impl.journal;

import com.hazelcast.projection.Projection;
import com.hazelcast.ringbuffer.impl.ReadResultSetImpl;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.function.Predicate;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class CacheEventJournalReadResultSetImpl<K, V, T> extends ReadResultSetImpl<InternalEventJournalCacheEvent, T> {

    CacheEventJournalReadResultSetImpl(int minSize, int maxSize, SerializationService serializationService,
                                       final Predicate<? super EventJournalCacheEvent<K, V>> predicate,
                                       final Projection<? super EventJournalCacheEvent<K, V>, T> projection) {
        super(minSize, maxSize, serializationService,
                predicate == null ? null : new Predicate<InternalEventJournalCacheEvent>() {
                    @Override
                    @SuppressWarnings("unchecked")
                    @SuppressFBWarnings("BC_UNCONFIRMED_CAST")
                    public boolean test(InternalEventJournalCacheEvent e) {
                        return predicate.test((DeserialisingEventJournalCacheEvent<K, V>) e);
                    }
                },
                projection == null ? null : new Projection<InternalEventJournalCacheEvent, T>() {
                    @Override
                    @SuppressWarnings("unchecked")
                    @SuppressFBWarnings("BC_UNCONFIRMED_CAST")
                    public T transform(InternalEventJournalCacheEvent input) {
                        return projection.transform((DeserialisingEventJournalCacheEvent<K, V>) input);
                    }
                });
    }

    @Override
    public void addItem(long seq, Object item) {
        final InternalEventJournalCacheEvent e = serializationService.toObject(item);
        final DeserialisingEventJournalCacheEvent<K, V> deserialisingEvent
                = new DeserialisingEventJournalCacheEvent<K, V>(serializationService, e);
        super.addItem(seq, deserialisingEvent);
    }

}
