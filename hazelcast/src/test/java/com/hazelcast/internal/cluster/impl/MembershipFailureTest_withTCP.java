/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.FirewallingNodeContext;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.After;
import org.junit.experimental.categories.Category;
import org.junit.runners.Parameterized;

import java.util.Collection;

import static com.hazelcast.instance.impl.HazelcastInstanceFactory.createInstanceName;

@Parameterized.UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category(SlowTest.class)
public class MembershipFailureTest_withTCP extends MembershipFailureTest {

    @After
    public void tearDown() {
        HazelcastInstanceFactory.terminateAll();
    }

    @Override
    HazelcastInstance newHazelcastInstance() {
        return newHazelcastInstance(new Config());
    }

    @Override
    HazelcastInstance newHazelcastInstance(Config config) {
        config.setProperty(ClusterProperty.HEARTBEAT_FAILURE_DETECTOR_TYPE.getName(), failureDetectorType.toString());
        initConfig(config);
        return HazelcastInstanceFactory.newHazelcastInstance(config, createInstanceName(config), new FirewallingNodeContext());
    }

    @Override
    Collection<HazelcastInstance> getAllHazelcastInstances() {
        return HazelcastInstanceFactory.getAllHazelcastInstances();
    }

    private static Config initConfig(Config config) {
        config.setProperty(ClusterProperty.WAIT_SECONDS_BEFORE_JOIN.getName(), "1");

        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);

        TcpIpConfig tcpIpConfig = join.getTcpIpConfig().setEnabled(true).clear();
        for (int i = 0; i < 10; i++) {
            int port = 5701 + i;
            tcpIpConfig.addMember("127.0.0.1:" + port);
        }

        return config;
    }
}
