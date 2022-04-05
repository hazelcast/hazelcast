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

package com.hazelcast.map.impl.querycache.accumulator;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.map.impl.querycache.QueryCacheEventService;
import com.hazelcast.map.impl.querycache.event.QueryCacheEventData;
import com.hazelcast.map.impl.querycache.event.sequence.Sequenced;
import com.hazelcast.map.impl.querycache.publisher.EventPublisherAccumulatorProcessor;
import com.hazelcast.map.impl.querycache.publisher.PublisherAccumulatorHandler;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * This class implements basic functionality of an {@link Accumulator}.
 * Subclasses should override required methods according to their context.
 *
 * @param <E> the type which will be accumulated in this {@link Accumulator}.
 * @see com.hazelcast.map.impl.querycache.publisher.NonStopPublisherAccumulator
 * @see com.hazelcast.map.impl.querycache.publisher.BatchPublisherAccumulator
 */
public class BasicAccumulator<E extends Sequenced> extends AbstractAccumulator<E> {

    protected final AccumulatorHandler<E> handler;
    protected final ILogger logger = Logger.getLogger(getClass());

    protected BasicAccumulator(QueryCacheContext context, AccumulatorInfo info) {
        super(context, info);
        this.handler = createAccumulatorHandler(context, info);
    }

    @Override
    public void accumulate(E event) {
        long sequence = partitionSequencer.nextSequence();
        event.setSequence(sequence);
        getBuffer().add(event);
    }

    @Override
    public int poll(AccumulatorHandler<E> handler, int maxItems) {
        if (maxItems < 1) {
            return 0;
        }

        CyclicBuffer<E> buffer = getBuffer();
        int size = size();
        if (size < 1 || size < maxItems) {
            return 0;
        }

        int count = 0;
        do {
            E current = buffer.getAndAdvance();
            if (current == null) {
                break;
            }
            count++;
            handler.handle(current, count == maxItems);
        } while (count < maxItems);

        return count;
    }

    @Override
    public int poll(AccumulatorHandler<E> handler, long delay, TimeUnit unit) {
        CyclicBuffer<E> buffer = getBuffer();
        if (size() < 1) {
            return 0;
        }

        long now = getNow();
        int count = 0;
        E next;
        do {
            E current = readCurrentExpiredOrNull(now, delay, unit);
            if (current == null) {
                break;
            }
            next = readNextExpiredOrNull(now, delay, unit);
            handler.handle(current, next == null);
            count++;
            buffer.getAndAdvance();
        } while (next != null);

        return count;
    }

    @Override
    public Iterator<E> iterator() {
        CyclicBuffer<E> buffer = getBuffer();
        return new ReadOnlyIterator<>(buffer);
    }

    @Override
    public int size() {
        return buffer.size();
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public AccumulatorInfo getInfo() {
        return info;
    }

    @Override
    public boolean setHead(long sequence) {
        return buffer.setHead(sequence);
    }

    @Override
    public void reset() {
        handler.reset();
        super.reset();
    }

    private E readNextExpiredOrNull(long now, long delay, TimeUnit unit) {
        long headSequence = buffer.getHeadSequence();
        headSequence++;
        E sequenced = buffer.get(headSequence);
        if (sequenced == null) {
            return null;
        }
        return isExpired((QueryCacheEventData) sequenced, unit.toMillis(delay), now) ? sequenced : null;
    }

    private E readCurrentExpiredOrNull(long now, long delay, TimeUnit unit) {
        long headSequence = buffer.getHeadSequence();
        E sequenced = buffer.get(headSequence);
        if (sequenced == null) {
            return null;
        }
        return isExpired((QueryCacheEventData) sequenced, unit.toMillis(delay), now) ? sequenced : null;
    }

    @SuppressWarnings("unchecked")
    protected AccumulatorHandler<E> createAccumulatorHandler(QueryCacheContext context, AccumulatorInfo info) {
        QueryCacheEventService queryCacheEventService = context.getQueryCacheEventService();
        AccumulatorProcessor<Sequenced> processor = createAccumulatorProcessor(info, queryCacheEventService);
        return (AccumulatorHandler<E>) new PublisherAccumulatorHandler(context, processor);
    }

    protected AccumulatorProcessor<Sequenced> createAccumulatorProcessor(AccumulatorInfo info,
                                                                         QueryCacheEventService eventService) {
        return new EventPublisherAccumulatorProcessor(info, eventService);
    }

    /**
     * Iterator used to read an {@link Accumulator}.
     *
     * @param <T> the type which can be stored in the {@link Accumulator}.
     */
    static class ReadOnlyIterator<T extends Sequenced> implements Iterator<T> {

        private final CyclicBuffer<T> buffer;

        ReadOnlyIterator(CyclicBuffer<T> buffer) {
            this.buffer = checkNotNull(buffer, "buffer cannot be null");
        }

        @Override
        public boolean hasNext() {
            return buffer.size() > 0;
        }

        @Override
        public T next() {
            return buffer.getAndAdvance();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Only read only iteration is allowed");
        }
    }
}
