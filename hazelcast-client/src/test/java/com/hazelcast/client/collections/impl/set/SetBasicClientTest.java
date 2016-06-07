package com.hazelcast.client.collections.impl.set;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.collection.impl.set.SetAbstractTest;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import org.junit.After;

public class SetBasicClientTest extends SetAbstractTest {
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
