package com.hazelcast.client.map.querycache;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.map.QueryCache;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.map.listener.EntryRemovedListener;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.TruePredicate;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;

import static com.hazelcast.map.impl.querycache.AbstractQueryCacheTestSupport.getMap;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientQueryCacheEventHandlingTest extends HazelcastTestSupport {

    @SuppressWarnings("unchecked")
    private static final Predicate<Integer, Integer> TRUE_PREDICATE = TruePredicate.INSTANCE;

    private TestHazelcastFactory factory = new TestHazelcastFactory();

    private IMap<Integer, Integer> map;
    private QueryCache<Integer, Integer> queryCache;

    @Before
    public void setUp() {
        factory.newHazelcastInstance();
        HazelcastInstance client = factory.newHazelcastClient();

        String mapName = randomMapName();
        map = getMap(client, mapName);
        queryCache = map.getQueryCache("cqc", TRUE_PREDICATE, true);
    }

    @After
    public void tearDown() throws Exception {
        factory.shutdownAll();
    }

    @Test
    public void testEvent_EXPIRED() throws Exception {
        int key = 1;
        int value = 1;

        final CountDownLatch latch = new CountDownLatch(1);
        queryCache.addEntryListener(new EntryAddedListener() {
            @Override
            public void entryAdded(EntryEvent event) {
                latch.countDown();
            }
        }, true);

        map.put(key, value, 1, SECONDS);

        latch.await();
        sleepSeconds(1);

        // map#get creates EXPIRED event
        map.get(key);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(0, queryCache.size());
            }
        });
    }

    @Test
    public void testListenerRegistration() {
        String addEntryListener = queryCache.addEntryListener(new EntryAddedListener<Integer, Integer>() {
            @Override
            public void entryAdded(EntryEvent<Integer, Integer> event) {
            }
        }, true);

        String removeEntryListener = queryCache.addEntryListener(new EntryRemovedListener<Integer, Integer>() {
            @Override
            public void entryRemoved(EntryEvent<Integer, Integer> event) {
            }
        }, true);

        assertFalse(queryCache.removeEntryListener("notFound"));

        assertTrue(queryCache.removeEntryListener(removeEntryListener));
        assertFalse(queryCache.removeEntryListener(removeEntryListener));

        assertTrue(queryCache.removeEntryListener(addEntryListener));
        assertFalse(queryCache.removeEntryListener(addEntryListener));
    }
}
