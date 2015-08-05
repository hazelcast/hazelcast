package com.hazelcast.collection.impl.set;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class SetMigrationTest extends HazelcastTestSupport {

    private Set<Object> set;
    private HazelcastInstance local;
    private HazelcastInstance remote1;
    private HazelcastInstance remote2;

    @Before
    public void setup() {
        HazelcastInstance[] cluster = createHazelcastInstanceFactory(3).newInstances();
        local = cluster[0];
        remote1 = cluster[1];
        remote2 = cluster[2];

        String name = randomNameOwnedBy(remote1);
        set = local.getSet(name);
    }

    @Test
    public void test() {
        for (int k = 0; k < 100; k++) {
            set.add(k);
        }

        remote1.shutdown();
        remote2.shutdown();

        assertEquals(100, set.size());

        for (int k = 0; k < 100; k++) {
            assertTrue("the set doesn't contain:" + k, set.contains(k));
        }
    }
}
