package com.hazelcast.map.impl.querycache.subscriber;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.eventservice.impl.EventServiceImpl;
import com.hazelcast.spi.impl.eventservice.impl.EventServiceSegment;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class NodeQueryCacheEventServiceTest extends HazelcastTestSupport {

    @Test
    public void no_left_over_listener_after_concurrent_addition_and_removal_on_same_queryCache() throws InterruptedException {
        final String mapName = "test";
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        final HazelcastInstance node = factory.newHazelcastInstance();

        SubscriberContext subscriberContext = getSubscriberContext(node);
        final NodeQueryCacheEventService nodeQueryCacheEventService = (NodeQueryCacheEventService) subscriberContext.getEventService();

        final AtomicBoolean stop = new AtomicBoolean(false);
        ArrayList<Thread> threads = new ArrayList<Thread>();
        for (int i = 0; i < 5; i++) {
            Thread thread = new Thread() {
                @Override
                public void run() {
                    while (!stop.get()) {
                        nodeQueryCacheEventService.addListener(mapName, "a", new EntryAddedListener() {
                            @Override
                            public void entryAdded(EntryEvent event) {

                            }
                        });

                        nodeQueryCacheEventService.removeAllListeners(mapName, "a");
                    }
                }
            };
            threads.add(thread);
        }

        for (Thread thread : threads) {
            thread.start();
        }

        sleepSeconds(5);
        stop.set(true);

        for (Thread thread : threads) {
            thread.join();
        }

        assertNoUserListenerLeft(node);
    }

    private static SubscriberContext getSubscriberContext(HazelcastInstance node) {
        MapService mapService = getNodeEngineImpl(node).getService(MapService.SERVICE_NAME);
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        QueryCacheContext queryCacheContext = mapServiceContext.getQueryCacheContext();
        return queryCacheContext.getSubscriberContext();
    }

    private static void assertNoUserListenerLeft(HazelcastInstance node) {
        NodeEngineImpl nodeEngineImpl = getNodeEngineImpl(node);
        EventServiceImpl eventServiceImpl = (EventServiceImpl) nodeEngineImpl.getEventService();
        EventServiceSegment segment = eventServiceImpl.getSegment(MapService.SERVICE_NAME, false);
        ConcurrentMap registrations = segment.getRegistrations();
        ConcurrentMap registrationIdMap = segment.getRegistrationIdMap();

        assertTrue(registrations.toString(), registrations.isEmpty());
        assertTrue(registrationIdMap.toString(), registrationIdMap.isEmpty());
    }
}
