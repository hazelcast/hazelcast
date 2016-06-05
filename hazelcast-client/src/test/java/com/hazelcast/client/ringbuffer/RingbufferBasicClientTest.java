package com.hazelcast.client.ringbuffer;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.ringbuffer.impl.RingbufferAbstractTest;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class RingbufferBasicClientTest extends RingbufferAbstractTest {
    private TestHazelcastFactory factory = new TestHazelcastFactory();

    @After
    public void teardown() {
        factory.terminateAll();
    }


    @Override
    protected HazelcastInstance[] newInstances(Config config) {
        HazelcastInstance member = factory.newHazelcastInstance(config);
        HazelcastInstance client = factory.newHazelcastClient();

        return new HazelcastInstance[]{client, member};
    }
}
