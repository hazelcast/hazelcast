package com.hazelcast.cluster;

import com.hazelcast.config.Config;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.OverridePropertyRule;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.test.OverridePropertyRule.clear;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class AutoDetectionJoinTest extends AbstractJoinTest {

    public final OverridePropertyRule ruleSysPropHazelcastLocalAddress = clear("hazelcast.local.localAddress");

    @Before
    @After
    public void killAllHazelcastInstances() {
        HazelcastInstanceFactory.terminateAll();
    }

    @Test
    public void defaultConfig() throws Exception {
        testJoinWithDefaultWait(new Config());
    }
}
