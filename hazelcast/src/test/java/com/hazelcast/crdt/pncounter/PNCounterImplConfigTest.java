package com.hazelcast.crdt.pncounter;

import com.hazelcast.config.CRDTReplicationConfig;
import com.hazelcast.config.ConfigurationException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class PNCounterImplConfigTest extends HazelcastTestSupport {

    @Test(expected = ConfigurationException.class)
    public void getProxyFailsWhenReplicationPeriodIsNegative() {
        new CRDTReplicationConfig().setReplicationPeriodMillis(-200);
    }

    @Test(expected = ConfigurationException.class)
    public void getProxyFailsWhenReplicationPeriodIsZero() {
        new CRDTReplicationConfig().setReplicationPeriodMillis(0);
    }

    @Test(expected = ConfigurationException.class)
    public void getProxyFailsWhenMaxTargetsIsNegative() {
        new CRDTReplicationConfig().setMaxConcurrentReplicationTargets(-200);
    }

    @Test(expected = ConfigurationException.class)
    public void getProxyFailsWhenMaxTargetsIsZero() {
        new CRDTReplicationConfig().setMaxConcurrentReplicationTargets(0);
    }
}