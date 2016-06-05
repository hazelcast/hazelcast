package com.hazelcast.collection.impl.set;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class SetBasicDistributedTest extends SetAbstractTest {

    private static final int INSTANCE_COUNT = 2;

    private TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory();


    @Override
    protected HazelcastInstance[] newInstances(Config config) {
        HazelcastInstance[] instances = new HazelcastInstance[INSTANCE_COUNT];
        for (int i = 0; i < INSTANCE_COUNT; i++) {
            instances[i] = factory.newHazelcastInstance(config);
        }
        return instances;
    }

    @After
    public void teardown() {
        factory.terminateAll();
    }
}
