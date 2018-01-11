/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.querycache.publisher;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.impl.querycache.QueryCacheEventService;
import com.hazelcast.map.impl.querycache.accumulator.AccumulatorInfo;
import com.hazelcast.map.impl.querycache.accumulator.AccumulatorProcessor;
import com.hazelcast.map.impl.querycache.event.sequence.Sequenced;

import static com.hazelcast.map.impl.querycache.ListenerRegistrationHelper.generateListenerName;

/**
 * Publish events which will be received by subscriber sides.
 *
 * @see AccumulatorProcessor
 */
public class EventPublisherAccumulatorProcessor implements AccumulatorProcessor<Sequenced> {

    private AccumulatorInfo info;
    private final QueryCacheEventService eventService;
    private final ILogger logger = Logger.getLogger(getClass());

    public EventPublisherAccumulatorProcessor(QueryCacheEventService eventService) {
        this(null, eventService);
    }

    public EventPublisherAccumulatorProcessor(AccumulatorInfo info, QueryCacheEventService eventService) {
        this.info = info;
        this.eventService = eventService;
    }

    @Override
    public void process(Sequenced sequenced) {
        String listenerName = generateListenerName(info.getMapName(), info.getCacheId());
        eventService.sendEventToSubscriber(listenerName, sequenced, sequenced.getPartitionId());

        if (logger.isFinestEnabled()) {
            logger.finest("Publisher sent events " + sequenced);
        }
    }

    public void setInfo(AccumulatorInfo info) {
        this.info = info;
    }
}
