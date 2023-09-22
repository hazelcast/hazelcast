/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.test.OverridePropertyRule;
import static com.hazelcast.spi.properties.ClusterProperty.ASYNC_JOIN_STRATEGY_ENABLED;
import static com.hazelcast.test.OverridePropertyRule.set;
import static com.hazelcast.test.TestEnvironment.HAZELCAST_TEST_USE_NETWORK;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import java.util.List;

/**
 * Make sure there are no delays when joining the cluster
 *
 * @author lprimak
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClusterJoinDelayTest extends HazelcastTestSupport {
    @Rule
    public final OverridePropertyRule overridePropertyRule = set(HAZELCAST_TEST_USE_NETWORK, "true");
    @Rule
    public final OverridePropertyRule overridePropertyRule2 = set(ASYNC_JOIN_STRATEGY_ENABLED.getName(), "true");

    private TestHazelcastInstanceFactory fact;
    private final int numInstances = 3;

    @Before
    public void beforeRun() {
        fact = createHazelcastInstanceFactory(numInstances);
    }

    @After
    public void afterRun() {
        fact.shutdownAll();
    }

    @Override
    protected Config getConfig() {
        Config config = new Config();
        // make sure the wait is longer than the tested-for delay.
        // here we make sure that the newHazelcastInstance() call returns w/o blocking
        config.setProperty(ClusterProperty.WAIT_SECONDS_BEFORE_JOIN.getName(), "120");
        config.setProperty(ClusterProperty.MAX_WAIT_SECONDS_BEFORE_JOIN.getName(), "480");
        TcpIpConfig tcpIpConfig = new TcpIpConfig();
        tcpIpConfig.setEnabled(true).addMember("localhost");
        config.getNetworkConfig().setPublicAddress("localhost").getJoin().setTcpIpConfig(tcpIpConfig);
        return config;
    }

    @Test
    public void noBlockingBeforeJoin() {
        HazelcastInstance hz1 = fact.newHazelcastInstance(getConfig());
        HazelcastInstance hz2 = fact.newHazelcastInstance(getConfig());
        HazelcastInstance hz3 = fact.newHazelcastInstance(getConfig());
        assertClusterSizeEventually(numInstances, List.of(hz1, hz2, hz3), 30);
    }
}
