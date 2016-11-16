package com.hazelcast.jet2.impl.deployment;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Ignore;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class ClientDeploymentTest extends AbstractDeploymentTest {

    private TestHazelcastFactory factory = new TestHazelcastFactory();

    @After
    public void tearDown() {
        factory.terminateAll();
    }


    @Override
    TestHazelcastInstanceFactory getFactory() {
        return factory;
    }

    @Override
    HazelcastInstance getHazelcastInstance() {
        return factory.newHazelcastClient();
    }
}
