package com.hazelcast.client.map;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.query.impl.predicates.SkipIndexAbstractIntegrationTest;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClientSkipIndexIntegrationTest extends SkipIndexAbstractIntegrationTest {
    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    @Override
    public HazelcastInstance getHazelcastInstance() {
        HazelcastInstance server = hazelcastFactory.newHazelcastInstance();
        return hazelcastFactory.newHazelcastClient();
    }
}
