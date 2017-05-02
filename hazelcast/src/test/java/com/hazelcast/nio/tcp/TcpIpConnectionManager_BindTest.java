/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.nio.tcp;


import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(HazelcastSerialClassRunner.class)
public class TcpIpConnectionManager_BindTest extends HazelcastTestSupport {


    @Before
    public void setup() {
        Hazelcast.shutdownAll();
    }

    @After
    public void after() {
        Hazelcast.shutdownAll();
    }

    @Test
    public void test() {
        Config config = new Config();
        config.setProperty(GroupProperty.SOCKET_ALLOW_ANY_PUBLIC_ADDRESS.getName(), "true");
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true).addMember("localhost");

        Config config2 = new Config();
        config2.setProperty(GroupProperty.SOCKET_ALLOW_ANY_PUBLIC_ADDRESS.getName(), "true");
        config2.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config2.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true).addMember("127.0.0.1:5702");

        Hazelcast.newHazelcastInstance(config2);
        final HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assert instance.getCluster().getMembers().size() == 2;
            }
        });
    }
}
