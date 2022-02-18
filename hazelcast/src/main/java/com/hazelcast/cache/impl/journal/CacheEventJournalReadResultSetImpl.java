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

package com.hazelcast.cache.impl.journal;

import com.hazelcast.cache.impl.CacheDataSerializerHook;
import com.hazelcast.cache.EventJournalCacheEvent;
import com.hazelcast.internal.serialization.SerializableByConvention;
import com.hazelcast.projection.Projection;
import com.hazelcast.ringbuffer.impl.ReadResultSetImpl;
import com.hazelcast.internal.serialization.SerializationService;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.function.Function;
import java.util.function.Predicate;

public class CacheEventJournalReadResultSetImpl<K, V, T> extends ReadResultSetImpl<InternalEventJournalCacheEvent, T> {

    public CacheEventJournalReadResultSetImpl() {
    }

    CacheEventJournalReadResultSetImpl(
            int minSize, int maxSize, SerializationService serializationService,
            final Predicate<? super EventJournalCacheEvent<K, V>> predicate,
            final Function<? super EventJournalCacheEvent<K, V>, ? extends T> projection
    ) {
        super(minSize, maxSize, serializationService,
                predicate == null ? null : new Predicate<InternalEventJournalCacheEvent>() {
                    @Override
                    @SuppressWarnings("unchecked")
                    @SuppressFBWarnings("BC_UNCONFIRMED_CAST")
                    public boolean test(InternalEventJournalCacheEvent e) {
                        return predicate.test((DeserializingEventJournalCacheEvent<K, V>) e);
                    }
                },
                projection == null ? null : new ProjectionAdapter<K, V, T>(projection));
    }

    @Override
    public void addItem(long seq, Object item) {
        // the event journal ringbuffer supports only OBJECT format for now
        final InternalEventJournalCacheEvent e = (InternalEventJournalCacheEvent) item;
        final DeserializingEventJournalCacheEvent<K, V> deserialisingEvent
                = new DeserializingEventJournalCacheEvent<K, V>(serializationService, e);
        super.addItem(seq, deserialisingEvent);
    }

    @Override
    public int getFactoryId() {
        return CacheDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return CacheDataSerializerHook.EVENT_JOURNAL_READ_RESULT_SET;
    }

    @SerializableByConvention
    private static class ProjectionAdapter<K, V, T> implements Projection<InternalEventJournalCacheEvent, T> {

        private final Function<? super EventJournalCacheEvent<K, V>, ? extends T> projection;

        ProjectionAdapter(Function<? super EventJournalCacheEvent<K, V>, ? extends T> projection) {
            this.projection = projection;
        }

        @Override
        @SuppressWarnings("unchecked")
        @SuppressFBWarnings("BC_UNCONFIRMED_CAST")
        public T transform(InternalEventJournalCacheEvent input) {
            return projection.apply((DeserializingEventJournalCacheEvent<K, V>) input);
        }
    }
}
