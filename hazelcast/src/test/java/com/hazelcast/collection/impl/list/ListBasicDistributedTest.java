package com.hazelcast.collection.impl.list;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class ListBasicDistributedTest extends ListBasicTest {

    @Override
    protected HazelcastInstance[] newInstances(Config config) {
        return createHazelcastInstanceFactory(2).newInstances(config);
    }
}
