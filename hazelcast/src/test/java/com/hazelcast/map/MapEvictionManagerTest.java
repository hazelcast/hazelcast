package com.hazelcast.map;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.NightlyTest;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.map.MapEvictionManager.SCHEDULER_INITIAL_DELAY;
import static com.hazelcast.map.MapEvictionManager.SCHEDULER_PERIOD;
import static com.hazelcast.map.MapEvictionManager.SCHEDULER_TIME_UNIT;
import static org.junit.Assert.assertTrue;

@RunWith (HazelcastParallelClassRunner.class)
@Category (NightlyTest.class)
public class MapEvictionManagerTest extends HazelcastTestSupport
{
    static final int MAX_SIZE = 100;

    final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(1);
    final HazelcastInstance instance = nodeFactory.newHazelcastInstance();

    final IMap<String, String> mapA = configureMap(randomMapName());
    final IMap<String, String> mapB = configureMap(randomMapName());

    final CountDownLatch aMapEntryAddedLatch = new CountDownLatch(1);
    final CountDownLatch bMapEntryAddedLatch = new CountDownLatch(1);
    final CountDownLatch entryEvictedLatch = new CountDownLatch(1);

    final AtomicReference<IMap> lastMapInEvictionLoop = new AtomicReference<IMap>();

    final EntryAdapter<String, String> aMapEntryAddedListener = new EntryAdapter<String, String>()
    {
        @Override
        public void entryAdded(EntryEvent<String, String> event)
        {
            lastMapInEvictionLoop.set(mapA);
            aMapEntryAddedLatch.countDown();
        }
    };

    final EntryAdapter<String, String> bMapEntryAddedListener = new EntryAdapter<String, String>()
    {
        @Override
        public void entryAdded(EntryEvent<String, String> event)
        {
            lastMapInEvictionLoop.set(mapB);
            bMapEntryAddedLatch.countDown();
        }
    };

    final EntryAdapter<String, String> entryEvictedListener = new EntryAdapter<String, String>()
    {
        @Override
        public void entryEvicted(EntryEvent<String, String> event)
        {
            entryEvictedLatch.countDown();
        }
    };


    @Test
    public void testEvictionDoesNotStopAtFirstNonEvictableMap() throws InterruptedException
    {
        //Because order of items in hash map is undefined we need to record it using add entry listener
        mapA.addEntryListener(aMapEntryAddedListener, true);
        mapB.addEntryListener(bMapEntryAddedListener, true);

        mapA.put(randomString(), randomString());
        mapB.put(randomString(), randomString());

        aMapEntryAddedLatch.await();
        bMapEntryAddedLatch.await();

        lastMapInEvictionLoop.get().addEntryListener(entryEvictedListener, true);

        //over populating last map
        for (int i = 0; i < MAX_SIZE * 2; i++)
        {
            lastMapInEvictionLoop.get().put(randomString(), randomString());
        }

        entryEvictedLatch.await(SCHEDULER_INITIAL_DELAY + SCHEDULER_PERIOD * 2, SCHEDULER_TIME_UNIT);

        assertTrueEventually(new AssertTask()
        {
            @Override
            public void run() throws Exception
            {
                assertTrue(lastMapInEvictionLoop.get().size() < MAX_SIZE);
            }
        }, SCHEDULER_TIME_UNIT.toSeconds(SCHEDULER_INITIAL_DELAY + SCHEDULER_PERIOD * 2));
    }


    private IMap<String, String> configureMap(String mapName)
    {
        instance.getConfig().getMapConfig(mapName)
                .setEvictionPolicy(MapConfig.EvictionPolicy.LRU)
                .setMaxSizeConfig(new MaxSizeConfig(MAX_SIZE, MaxSizeConfig.MaxSizePolicy.PER_NODE));

        return instance.getMap(mapName);
    }
}
