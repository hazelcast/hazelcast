package com.hazelcast.cp.internal;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class CPGroupRebalanceTest extends HazelcastRaftTestSupport {

    @Test
    public void test() {
        Config config = createConfig(5, 3);
        HazelcastInstance[] instances = factory.newInstances(config, 5);
        waitUntilCPDiscoveryCompleted(instances);

        RaftInvocationManager invocationManager = getRaftService(instances[0]).getInvocationManager();

        for (int i = 0; i < 10; i++) {
            invocationManager.createRaftGroup("group-" + i);
        }

        sleepSeconds(30);
    }
}
