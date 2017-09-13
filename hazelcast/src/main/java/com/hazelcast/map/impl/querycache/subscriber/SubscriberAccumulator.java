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

package com.hazelcast.map.impl.querycache.subscriber;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.map.impl.querycache.accumulator.AccumulatorHandler;
import com.hazelcast.map.impl.querycache.accumulator.AccumulatorInfo;
import com.hazelcast.map.impl.querycache.accumulator.BasicAccumulator;
import com.hazelcast.map.impl.querycache.event.QueryCacheEventData;
import com.hazelcast.map.impl.querycache.event.sequence.DefaultSubscriberSequencerProvider;
import com.hazelcast.map.impl.querycache.event.sequence.Sequenced;
import com.hazelcast.map.impl.querycache.event.sequence.SubscriberSequencerProvider;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.map.impl.querycache.subscriber.EventPublisherHelper.publishEventLost;
import static java.lang.String.format;

/**
 * If all incoming events are in the correct sequence order, this accumulator applies those events to
 * {@link com.hazelcast.map.QueryCache QueryCache}.
 * Otherwise, it informs registered callback if there is any.
 * <p>
 * This class can be accessed by multiple-threads at a time.
 */
public class SubscriberAccumulator extends BasicAccumulator<QueryCacheEventData> {

    /**
     * When a partition's sequence order is broken, it will be registered here.
     */
    private final ConcurrentMap<Integer, Long> brokenSequences = new ConcurrentHashMap<Integer, Long>();

    private final AccumulatorHandler handler;
    private final SubscriberSequencerProvider sequenceProvider;

    protected SubscriberAccumulator(QueryCacheContext context, AccumulatorInfo info) {
        super(context, info);

        this.handler = createAccumulatorHandler();
        this.sequenceProvider = createSequencerProvider();
    }

    @Override
    public void accumulate(QueryCacheEventData event) {
        if (logger.isFinestEnabled()) {
            logger.finest("Received event=" + event);
        }

        if (!isApplicable(event)) {
            if (logger.isFinestEnabled()) {
                logger.finest("Event was not inserted to queryCache=" + event);
            }

            return;
        }

        if (logger.isFinestEnabled()) {
            logger.finest("Event was added to queryCache=" + event);
        }

        addQueryCache(event);
    }

    /**
     * Checks whether the event data is applicable to the query cache.
     */
    private boolean isApplicable(QueryCacheEventData event) {
        if (!getInfo().isPublishable()) {
            return false;
        }

        final int partitionId = event.getPartitionId();

        if (isEndEvent(event)) {
            sequenceProvider.reset(partitionId);
            removeFromBrokenSequences(event);
            return false;
        }

        if (isNextEvent(event)) {
            long currentSequence = sequenceProvider.getSequence(partitionId);
            sequenceProvider.compareAndSetSequence(currentSequence, event.getSequence(), partitionId);

            removeFromBrokenSequences(event);
            return true;
        }

        handleUnexpectedEvent(event);

        return false;
    }

    private void removeFromBrokenSequences(QueryCacheEventData event) {
        if (brokenSequences.isEmpty()) {
            return;
        }

        int partitionId = event.getPartitionId();
        long sequence = event.getSequence();

        if (sequence == -1L) {
            brokenSequences.remove(partitionId);
        } else {
            Long expected = brokenSequences.get(partitionId);
            if (expected != null && expected == event.getSequence()) {
                brokenSequences.remove(partitionId);
            }
        }

        if (logger.isFinestEnabled()) {
            logger.finest(format("Size of broken sequences=%d", brokenSequences.size()));
        }
    }

    private void handleUnexpectedEvent(QueryCacheEventData event) {
        addEventSequenceToBrokenSequences(event);
        publishEventLost(context, info.getMapName(), info.getCacheId(), event.getPartitionId());
    }

    private void addEventSequenceToBrokenSequences(QueryCacheEventData event) {
        Long prev = brokenSequences.putIfAbsent(event.getPartitionId(), event.getSequence());

        if (prev == null && logger.isFinestEnabled()) {
            logger.finest(format("Added unexpected event sequence to broken sequences "
                            + "[partitionId=%d, expected-sequence=%d, broken-sequences-size=%d]",
                    event.getPartitionId(), event.getSequence(), brokenSequences.size()));
        }
    }

    protected boolean isNextEvent(Sequenced event) {
        int partitionId = event.getPartitionId();
        long currentSequence = sequenceProvider.getSequence(partitionId);

        long foundSequence = event.getSequence();
        long expectedSequence = currentSequence + 1L;

        boolean isNextSequence = foundSequence == expectedSequence;

        if (!isNextSequence) {
            if (logger.isWarningEnabled()) {
                InternalQueryCache queryCache = getQueryCache();
                if (queryCache != null) {
                    logger.warning(format("Event lost detected for partitionId=%d, expectedSequence=%d "
                                    + "but foundSequence=%d, cacheSize=%d",
                            partitionId, expectedSequence, foundSequence, queryCache.size()));
                }
            }
        }

        return isNextSequence;
    }

    private InternalQueryCache getQueryCache() {
        AccumulatorInfo info = getInfo();
        String cacheId = info.getCacheId();
        SubscriberContext subscriberContext = context.getSubscriberContext();
        QueryCacheFactory queryCacheFactory = subscriberContext.getQueryCacheFactory();
        return queryCacheFactory.getOrNull(cacheId);
    }

    private SubscriberAccumulatorHandler createAccumulatorHandler() {
        AccumulatorInfo info = getInfo();
        boolean includeValue = info.isIncludeValue();
        InternalQueryCache queryCache = getQueryCache();
        InternalSerializationService serializationService = context.getSerializationService();
        return new SubscriberAccumulatorHandler(includeValue, queryCache, serializationService);
    }

    private void addQueryCache(QueryCacheEventData eventData) {
        handler.handle(eventData, false);
    }

    protected SubscriberSequencerProvider createSequencerProvider() {
        return new DefaultSubscriberSequencerProvider();
    }

    public ConcurrentMap<Integer, Long> getBrokenSequences() {
        return brokenSequences;
    }

    public boolean isEndEvent(QueryCacheEventData event) {
        return event.getSequence() == -1L;
    }
}
