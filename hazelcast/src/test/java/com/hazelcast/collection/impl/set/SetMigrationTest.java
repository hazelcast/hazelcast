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

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class SetMigrationTest extends HazelcastTestSupport {

    private Set<Object> set;
    private HazelcastInstance remote1;
    private HazelcastInstance remote2;

    @Before
    public void setup() {
        HazelcastInstance[] cluster = createHazelcastInstanceFactory(3).newInstances();
        HazelcastInstance local = cluster[0];
        remote1 = cluster[1];
        remote2 = cluster[2];

        String name = randomNameOwnedBy(remote1);
        set = local.getSet(name);
    }

    @Test
    public void test() {
        int size = 100;
        for (int element = 0; element < size; element++) {
            set.add(element);
        }

        remote1.shutdown();
        remote2.shutdown();

        assertEquals(size, set.size());
        for (int element = 0; element < size; element++) {
            assertContains(set, element);
        }
    }
}
