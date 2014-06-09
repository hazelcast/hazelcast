package com.hazelcast.map;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.NightlyTest;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.map.MapEvictionManager.SCHEDULER_INITIAL_DELAY;
import static com.hazelcast.map.MapEvictionManager.SCHEDULER_PERIOD;
import static com.hazelcast.map.MapEvictionManager.SCHEDULER_TIME_UNIT;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@RunWith (HazelcastParallelClassRunner.class)
@Category (NightlyTest.class)
public class MapEvictionManagerTest extends HazelcastTestSupport
{
    static final int MAX_SIZE = 100;

    final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(1);
    final HazelcastInstance instance = nodeFactory.newHazelcastInstance();

    final CountDownLatch entryEvictedLatch = new CountDownLatch(1);

    final EntryAdapter<String, String> entryEvictedListener = new EntryAdapter<String, String>()
    {
        @Override
        public void entryEvicted(EntryEvent<String, String> event)
        {
            entryEvictedLatch.countDown();
        }
    };

    @Before
    public void setUp()
    {
        configureMap("Alice");
        configureMap("Bob");
    }


    @Test
    public void testEvictionDoesNotStopAtFirstNonEvictableMap() throws InterruptedException
    {
        final List<DistributedObject> distributedObjects = new ArrayList<DistributedObject>(
                instance.getDistributedObjects());
        assertThat(distributedObjects.size(), equalTo(2));

        final IMap lastMapInEvictionLoop = (IMap) distributedObjects.get(1);

        lastMapInEvictionLoop.addEntryListener(entryEvictedListener, true);

        //over populating map
        for (int i = 0; i < MAX_SIZE * 2; i++)
        {
            lastMapInEvictionLoop.put(randomString(), randomString());
        }

        entryEvictedLatch.await(SCHEDULER_INITIAL_DELAY + SCHEDULER_PERIOD * 2, SCHEDULER_TIME_UNIT);

        assertTrueEventually(new AssertTask()
        {
            @Override
            public void run() throws Exception
            {
                assertTrue(lastMapInEvictionLoop.size() < MAX_SIZE);
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
