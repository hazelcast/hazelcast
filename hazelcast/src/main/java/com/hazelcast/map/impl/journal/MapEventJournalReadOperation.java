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

package com.hazelcast.map.impl.journal;

import com.hazelcast.internal.journal.EventJournal;
import com.hazelcast.internal.journal.EventJournalReadOperation;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.EventJournalMapEvent;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.ringbuffer.impl.ReadResultSetImpl;

import java.io.IOException;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Reads from the map event journal in batches. You may specify the start sequence,
 * the minumum required number of items in the response, the maximum number of items
 * in the response, a predicate that the events should pass and a projection to
 * apply to the events in the journal.
 * If the event journal currently contains less events than the required minimum, the
 * call will wait until it has sufficient items.
 * The predicate, filter and projection may be {@code null} in which case all elements are returned
 * and no projection is applied.
 *
 * @param <T> the return type of the projection. It is equal to an implementation of {@link }
 *            if the projection is {@code null} or it is the identity projection
 * @since 3.9
 */
public class MapEventJournalReadOperation<K, V, T> extends EventJournalReadOperation<T, InternalEventJournalMapEvent> {

    protected Predicate<? super EventJournalMapEvent<K, V>> predicate;
    protected Function<? super EventJournalMapEvent<K, V>, ? extends T> projection;

    public MapEventJournalReadOperation() {
    }

    public MapEventJournalReadOperation(
            String mapName, long startSequence, int minSize, int maxSize,
            Predicate<? super EventJournalMapEvent<K, V>> predicate,
            Function<? super EventJournalMapEvent<K, V>, ? extends T> projection
    ) {
        super(mapName, startSequence, minSize, maxSize);
        this.predicate = predicate;
        this.projection = projection;
    }

    @Override
    protected ReadResultSetImpl<InternalEventJournalMapEvent, T> createResultSet() {
        return new MapEventJournalReadResultSetImpl<>(
                minSize, maxSize, getNodeEngine().getSerializationService(), predicate, projection);
    }

    @Override
    protected EventJournal<InternalEventJournalMapEvent> getJournal() {
        final MapService service = getService();
        return service.getMapServiceContext().getEventJournal();
    }


    @Override
    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.EVENT_JOURNAL_READ;
    }

    @Override
    public String getServiceName() {
        return MapService.SERVICE_NAME;
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
}
