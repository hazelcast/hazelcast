package com.hazelcast.client.pncounter;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.ConsistencyLostException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.crdt.pncounter.BasePNCounterNoDataMemberTest;
import com.hazelcast.crdt.pncounter.PNCounter;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * Client implementation for testing behaviour of {@link ConsistencyLostException}
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientPNCounterNoDataMemberTest extends BasePNCounterNoDataMemberTest {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    private String counterName = randomMapName("counter-");
    private HazelcastInstance client;

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    @Before
    public void setup() {
        hazelcastFactory.newHazelcastInstance(new Config().setLiteMember(true));
        client = hazelcastFactory.newHazelcastClient();
    }

    @Override
    protected void mutate(PNCounter driver) {
        driver.addAndGet(5);
    }

    @Override
    protected PNCounter getCounter() {
        return client.getPNCounter(counterName);
    }
}