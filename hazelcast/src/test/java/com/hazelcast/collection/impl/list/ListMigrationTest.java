package com.hazelcast.collection.impl.list;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class ListMigrationTest extends HazelcastTestSupport{

    private List list;
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
        list = local.getList(name);
    }

    @Test
    public void test() {
        for (int k = 0; k < 100; k++) {
            list.add(k);
        }

        remote1.shutdown();
        remote2.shutdown();

        assertEquals(100, list.size());

        for (int k = 0; k < 100; k++) {
            assertEquals("the set doesn't contain:" + k, k, list.get(k));
        }
    }
}
