package com.hazelcast.client.pncounter;

import com.hazelcast.client.proxy.ClientPNCounterProxy;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.CRDTReplicationConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.ConsistencyLostException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.crdt.pncounter.BasePNCounterConsistencyLostTest;
import com.hazelcast.crdt.pncounter.PNCounter;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

/**
 * Client implementation for testing behaviour of {@link ConsistencyLostException}
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientPNCounterConsistencyLostTest extends BasePNCounterConsistencyLostTest {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    private String counterName = randomMapName("counter-");
    private HazelcastInstance[] members;
    private HazelcastInstance client;

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    @Before
    public void setup() {
        final Config config = new Config()
                .setProperty(GroupProperty.PARTITION_COUNT.getName(), "5")
                .setCRDTReplicationConfig(new CRDTReplicationConfig()
                        .setReplicationPeriodMillis(Integer.MAX_VALUE)
                        .setMaxConcurrentReplicationTargets(Integer.MAX_VALUE));
        members = hazelcastFactory.newInstances(config, 2);
        client = hazelcastFactory.newHazelcastClient();
    }


    @Override
    protected HazelcastInstance[] getMembers() {
        return members;
    }

    @Override
    protected Address getCurrentTargetReplicaAddress(PNCounter driver) {
        return ((ClientPNCounterProxy) driver).getCurrentTargetReplicaAddress();
    }

    @Override
    protected void assertState(PNCounter driver) {
        assertEquals(5, driver.get());
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