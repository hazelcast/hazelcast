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

package com.hazelcast.map.impl.querycache.subscriber;

import com.hazelcast.map.IMapEvent;
import com.hazelcast.map.impl.ListenerAdapter;
import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.map.impl.querycache.accumulator.Accumulator;
import com.hazelcast.map.impl.querycache.accumulator.AccumulatorInfo;
import com.hazelcast.map.impl.querycache.event.BatchEventData;
import com.hazelcast.map.impl.querycache.event.BatchIMapEvent;
import com.hazelcast.map.impl.querycache.event.QueryCacheEventData;
import com.hazelcast.map.impl.querycache.event.SingleIMapEvent;
import com.hazelcast.internal.serialization.SerializationService;

import java.util.Collection;

/**
 * Subscriber side listener per {@code QueryCache} which listens events sent by publisher-sides.
 */
public class SubscriberListener implements ListenerAdapter<IMapEvent> {

    private final AccumulatorInfo info;
    private final Accumulator accumulator;
    private final SubscriberContext subscriberContext;
    private final SerializationService serializationService;

    public SubscriberListener(QueryCacheContext context, AccumulatorInfo info) {
        this.info = info;
        this.subscriberContext = context.getSubscriberContext();
        this.accumulator = createAccumulator();
        this.serializationService = context.getSerializationService();
    }

    @Override
    public void onEvent(IMapEvent iMapEvent) {
        if (iMapEvent instanceof SingleIMapEvent) {
            QueryCacheEventData eventData = ((SingleIMapEvent) iMapEvent).getEventData();
            eventData.setSerializationService(serializationService);
            accumulator.accumulate(eventData);
            return;
        }

        if (iMapEvent instanceof BatchIMapEvent) {
            BatchIMapEvent batchIMapEvent = (BatchIMapEvent) iMapEvent;
            BatchEventData batchEventData = batchIMapEvent.getBatchEventData();
            Collection<QueryCacheEventData> events = batchEventData.getEvents();
            for (QueryCacheEventData eventData : events) {
                eventData.setSerializationService(serializationService);
                accumulator.accumulate(eventData);
            }
            return;
        }
    }

    private Accumulator createAccumulator() {
        MapSubscriberRegistry mapSubscriberRegistry = subscriberContext.getMapSubscriberRegistry();
        SubscriberRegistry subscriberRegistry = mapSubscriberRegistry.getOrCreate(info.getMapName());
        return subscriberRegistry.getOrCreate(info.getCacheId());
    }
}
