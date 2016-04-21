package com.hazelcast.map;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class LocalMapStatsTest_WhenMigration extends HazelcastTestSupport {

    private HazelcastInstance hz1;
    private HazelcastInstance hz2;

    private TestHazelcastInstanceFactory factory;

    private IMap<Integer, Integer> map;

    @Before
    public void setUp() {
        factory = createHazelcastInstanceFactory(2);
        hz1 = factory.newHazelcastInstance();

        map = hz1.getMap("trial");
    }

    @Test
    public void testHitsGenerated_newNode() throws Exception {
        for (int i = 0; i < 100; i++) {
            map.put(i, i);
            map.get(i);
        }

        hz2 = factory.newHazelcastInstance();
        final IMap<Object, Object> trial = hz2.getMap("trial");

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                long hits2 = trial.getLocalMapStats().getHits();
                long hits1 = map.getLocalMapStats().getHits();

                assertEquals(100, hits1 + hits2);
            }
        });
    }

    @Test
    public void testHitsGenerated_nodeCrash() throws Exception {

        for (int i = 0; i < 100; i++) {
            map.put(i, i);
            map.get(i);
        }

        hz2 = factory.newHazelcastInstance();

        waitAllForSafeState(factory.getAllHazelcastInstances());
        factory.terminate(hz2);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                long hits = map.getLocalMapStats().getHits();
                assertEquals(100, hits);
            }
        });
    }
}