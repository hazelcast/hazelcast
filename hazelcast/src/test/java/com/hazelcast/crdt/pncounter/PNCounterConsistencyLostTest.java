package com.hazelcast.crdt.pncounter;

import com.hazelcast.config.CRDTReplicationConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.ConsistencyLostException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

/**
 * Member implementation for testing behaviour of {@link ConsistencyLostException}
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class PNCounterConsistencyLostTest extends BasePNCounterConsistencyLostTest {

    private HazelcastInstance[] members;
    private HazelcastInstance liteMember;

    @Before
    public void setup() {
        final Config dataConfig = new Config()
                .setProperty(GroupProperty.PARTITION_COUNT.getName(), "5")
                .setCRDTReplicationConfig(new CRDTReplicationConfig().setReplicationPeriodMillis(Integer.MAX_VALUE)
                                                                     .setMaxConcurrentReplicationTargets(Integer.MAX_VALUE));
        final Config liteConfig = new Config()
                .setProperty(GroupProperty.PARTITION_COUNT.getName(), "5")
                .setLiteMember(true);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        members = factory.newInstances(dataConfig, 2);
        liteMember = factory.newHazelcastInstance(liteConfig);
    }


    @Override
    protected HazelcastInstance[] getMembers() {
        return members;
    }

    @Override
    protected Address getCurrentTargetReplicaAddress(PNCounter driver) {
        return ((PNCounterProxy) driver).getCurrentTargetReplicaAddress();
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
        final PNCounter counter = liteMember.getPNCounter("counter");
        ((PNCounterProxy) counter).setOperationTryCount(1);
        return counter;
    }
}