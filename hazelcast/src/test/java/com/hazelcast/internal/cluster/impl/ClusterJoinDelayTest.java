/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.internal.cluster.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import static com.hazelcast.test.TestEnvironment.HAZELCAST_TEST_USE_NETWORK;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import java.util.Arrays;
import org.junit.After;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * Make sure there are no delays when joining the cluster
 *
 * @author lprimak
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClusterJoinDelayTest extends HazelcastTestSupport {
    private HazelcastInstance[] hzInst;
    private TestHazelcastInstanceFactory fact;
    private final int numInstances = 3;

    @BeforeClass
    public static void init() {
        System.setProperty(HAZELCAST_TEST_USE_NETWORK, Boolean.TRUE.toString());
    }

    @Before
    public void beforeRun() {
        fact = createHazelcastInstanceFactory(numInstances);
        hzInst = new HazelcastInstance[numInstances];
    }

    @After
    public void afterRun() {
        Arrays.stream(hzInst).forEach(HazelcastInstance::shutdown);
    }

    @Override
    protected Config getConfig() {
        Config config = new Config();
        // make sure the wait is longer than the tesed-for delay.
        // here we make sure that the newHazelcastInstance() call returns w/o blocking
        config.setProperty(ClusterProperty.WAIT_SECONDS_BEFORE_JOIN.getName(), "5");
        config.setProperty(ClusterProperty.MAX_WAIT_SECONDS_BEFORE_JOIN.getName(), "20");
        TcpIpConfig tcpIpConfig = new TcpIpConfig();
        tcpIpConfig.setEnabled(true).addMember("localhost");
        config.getNetworkConfig().setPublicAddress("localhost").getJoin().setTcpIpConfig(tcpIpConfig);
        return config;
    }

    // below timeout should be 4 (one less than WAIT_SECONDS_BEFORE_JOIN)
    // however, since async joins don't work, one more than the above works
    @Test(timeout = 6 * 1000)
    public void testJoinDelayLessThanFourSeconds() {
        hzInst[0] = fact.newHazelcastInstance(getConfig());
        hzInst[1] = fact.newHazelcastInstance(getConfig());
        hzInst[2] = fact.newHazelcastInstance(getConfig());
        assertTrue("hz1 should always be numInstances members here", hzInst[0].getCluster().getMembers().size() == numInstances);
        assertTrue("hz2 should always be numInstances members here", hzInst[1].getCluster().getMembers().size() == numInstances);
    }
}
