package com.hazelcast.map.impl.querycache.subscriber;

import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.map.impl.querycache.accumulator.Accumulator;
import com.hazelcast.map.impl.querycache.accumulator.AccumulatorInfo;
import com.hazelcast.map.impl.querycache.event.DefaultQueryCacheEventData;
import com.hazelcast.map.impl.querycache.event.sequence.Sequenced;

import java.util.Collections;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class TestSubscriberContext extends NodeSubscriberContext {

    private final MapSubscriberRegistry mapSubscriberRegistry;
    private final int eventCount;
    private final boolean enableEventLoss;

    public TestSubscriberContext(QueryCacheContext context, int eventCount, boolean enableEventLoss) {
        super(context);
        this.eventCount = eventCount;
        this.enableEventLoss = enableEventLoss;
        this.mapSubscriberRegistry = new TestMapSubscriberRegistry(context);
    }

    @Override
    public MapSubscriberRegistry getMapSubscriberRegistry() {
        return mapSubscriberRegistry;
    }

    private class TestMapSubscriberRegistry extends MapSubscriberRegistry {

        TestMapSubscriberRegistry(QueryCacheContext context) {
            super(context);
        }

        @Override
        protected SubscriberRegistry createSubscriberRegistry(String mapName) {
            return new TestSubscriberRegistry(getContext(), mapName);
        }
    }

    private class TestSubscriberRegistry extends SubscriberRegistry {

        TestSubscriberRegistry(QueryCacheContext context, String mapName) {
            super(context, mapName);
        }

        @Override
        protected SubscriberAccumulatorFactory createSubscriberAccumulatorFactory() {
            return new TestSubscriberAccumulatorFactory(getContext());
        }
    }

    private class TestSubscriberAccumulatorFactory extends SubscriberAccumulatorFactory {

        TestSubscriberAccumulatorFactory(QueryCacheContext context) {
            super(context);
        }

        @Override
        public Accumulator createAccumulator(AccumulatorInfo info) {
            return new TestSubscriberAccumulator(getContext(), info);
        }
    }

    private class TestSubscriberAccumulator extends SubscriberAccumulator {

        private final Set<Long> lostSequenceNumber = Collections.newSetFromMap(new ConcurrentHashMap<Long, Boolean>());

        TestSubscriberAccumulator(QueryCacheContext context, AccumulatorInfo info) {
            super(context, info);

            if (enableEventLoss) {
                // just pick a sequence number to mimic out of order events
                lostSequenceNumber.add(new Random().nextInt(eventCount) + 1L);
            }
        }

        @Override
        protected boolean isNextEvent(Sequenced event) {
            DefaultQueryCacheEventData eventData = (DefaultQueryCacheEventData) event;
            if (lostSequenceNumber.remove(event.getSequence())) {
                // create an out of order event by changing actual sequence
                DefaultQueryCacheEventData copy = new DefaultQueryCacheEventData(eventData);
                copy.setSequence(eventData.getSequence() * 2);

                eventData = copy;
            }
            return super.isNextEvent(eventData);
        }
    }
}
