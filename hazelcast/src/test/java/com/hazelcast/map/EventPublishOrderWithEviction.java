package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.Assert.assertEquals;


@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class EventPublishOrderWithEviction extends HazelcastTestSupport {

    @Test
    public void testEntryEvictEventsEmitted_afterAddEvents() throws Exception {
        final int maxSize = 10;
        IMap<Integer, Integer> map = createMap(maxSize);
        EventOrderAwareEntryListener entryListener = new EventOrderAwareEntryListener();
        map.addEntryListener(entryListener, true);

        for (int j = 0; j < 3; j++) {
            for (int i = 0; i < maxSize; i++) {
                map.put(i, i);
            }
        }

        sleepMillis(3456);

        assertEmittedEventsOrder(entryListener);
    }

    private void assertEmittedEventsOrder(EventOrderAwareEntryListener entryListener) {
        Map<Integer, List<EntryEventType>> eventsPerKey = new HashMap<Integer, List<EntryEventType>>();
        List<EntryEvent> events = entryListener.getOrderedEvents();
        for (EntryEvent event : events) {
            Integer key = (Integer) event.getKey();
            List<EntryEventType> eventTypes = eventsPerKey.get(key);
            if (eventTypes == null) {
                eventTypes = new ArrayList<EntryEventType>();
                eventsPerKey.put(key, eventTypes);
            }

            EntryEventType eventType = event.getEventType();
            eventTypes.add(eventType);
        }

        Set<Map.Entry<Integer, List<EntryEventType>>> entries = eventsPerKey.entrySet();
        for (Map.Entry<Integer, List<EntryEventType>> entry : entries) {
            List<EntryEventType> eventTypes = entry.getValue();
            EntryEventType prev = null;
            for (int i = 0; i < eventTypes.size(); i++) {
                final EntryEventType eventType = eventTypes.get(i);
                if (i == 0) {
                    assertEquals(EntryEventType.ADDED, eventType);
                    prev = eventType;
                    continue;
                }

                if (prev.equals(EntryEventType.ADDED)) {
                    assertEquals(EntryEventType.EVICTED, eventType);
                } else if (prev.equals(EntryEventType.EVICTED)) {
                    assertEquals(EntryEventType.ADDED, eventType);
                }

                prev = eventType;
            }
        }
    }

    private <K, V> IMap<K, V> createMap(int maxSize) {
        String mapName = randomMapName();
        Config config = new Config();
        MapConfig mapConfig = config.getMapConfig(mapName);
        mapConfig.setEvictionPolicy(EvictionPolicy.LRU);
        MaxSizeConfig maxSizeConfig = new MaxSizeConfig();
        maxSizeConfig.setSize(maxSize);
        mapConfig.setMaxSizeConfig(maxSizeConfig);
        mapConfig.setMinEvictionCheckMillis(0);

        return createHazelcastInstance(config).getMap(mapName);
    }


    private static final class EventOrderAwareEntryListener extends EntryAdapter {

        private final List<EntryEvent> orderedEvents = new CopyOnWriteArrayList<EntryEvent>();

        @Override
        public void entryEvicted(EntryEvent event) {
            orderedEvents.add(event);
        }

        @Override
        public void entryAdded(EntryEvent event) {
            orderedEvents.add(event);
        }

        public List<EntryEvent> getOrderedEvents() {
            return orderedEvents;
        }
    }
}