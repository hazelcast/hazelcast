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
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.EventJournalCacheEvent;
import com.hazelcast.internal.journal.EventJournal;
import com.hazelcast.internal.journal.EventJournalReadOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.ringbuffer.impl.ReadResultSetImpl;

import java.io.IOException;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Reads from the cache event journal in batches. You may specify the start sequence,
 * the minumum required number of items in the response, the maximum number of items
 * in the response, a predicate that the events should pass and a projection to
 * apply to the events in the journal.
 * If the event journal currently contains less events than the required minimum, the
 * call will wait until it has sufficient items.
 * The predicate, filter and projection may be {@code null} in which case all elements are returned
 * and no projection is applied.
 *
 * @param <T> the return type of the projection. It is equal to the journal event type
 *            if the projection is {@code null} or it is the identity projection
 * @since 3.9
 */
public class CacheEventJournalReadOperation<K, V, T>
        extends EventJournalReadOperation<T, InternalEventJournalCacheEvent> {
    protected Predicate<? super EventJournalCacheEvent<K, V>> predicate;
    protected Function<? super EventJournalCacheEvent<K, V>, ? extends T> projection;

    public CacheEventJournalReadOperation() {
    }

    public CacheEventJournalReadOperation(
            String cacheName, long startSequence, int minSize, int maxSize,
            Predicate<? super EventJournalCacheEvent<K, V>> predicate,
            Function<? super EventJournalCacheEvent<K, V>, ? extends T> projection
    ) {
        super(cacheName, startSequence, minSize, maxSize);
        this.predicate = predicate;
        this.projection = projection;
    }

    @Override
    protected EventJournal<InternalEventJournalCacheEvent> getJournal() {
        final CacheService service = getService();
        return service.getEventJournal();
    }


    @Override
    public int getFactoryId() {
        return CacheDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return CacheDataSerializerHook.EVENT_JOURNAL_READ_OPERATION;
    }

    @Override
    public String getServiceName() {
        return CacheService.SERVICE_NAME;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(predicate);
        out.writeObject(projection);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        predicate = in.readObject();
        projection = in.readObject();
    }

    @Override
    protected ReadResultSetImpl<InternalEventJournalCacheEvent, T> createResultSet() {
        return new CacheEventJournalReadResultSetImpl<K, V, T>(
                minSize, maxSize, getNodeEngine().getSerializationService(), predicate, projection);
    }
}
