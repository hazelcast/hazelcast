package com.hazelcast.client.lock;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.concurrent.lock.ConditionAbstractTest;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientConditionTest extends ConditionAbstractTest {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @Override
    protected HazelcastInstance[] newInstances() {
        HazelcastInstance member = hazelcastFactory.newHazelcastInstance();
        HazelcastInstance client = hazelcastFactory.newHazelcastClient();

        return new HazelcastInstance[]{client, member};
    }

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }
}
