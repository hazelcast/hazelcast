/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.networking.nio;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.nio.tcp.TcpIpConnectionManager;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.spi.properties.GroupProperty.MERGE_FIRST_RUN_DELAY_SECONDS;
import static com.hazelcast.spi.properties.GroupProperty.MERGE_NEXT_RUN_DELAY_SECONDS;
import static junit.framework.TestCase.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class NioChannelMemoryLeakTest extends HazelcastTestSupport {


    @After
    public void cleanUp() {
        Hazelcast.shutdownAll();
    }

    @Test
    public void testNioChannelLeak() {
        Config config = new Config();
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getTcpIpConfig().addMember("127.0.0.1:6000").setEnabled(true);
        join.getMulticastConfig().setEnabled(false);

        config.setProperty(MERGE_NEXT_RUN_DELAY_SECONDS.getName(), "1");
        config.setProperty(MERGE_FIRST_RUN_DELAY_SECONDS.getName(), "1");
        HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
        TcpIpConnectionManager connectionManager = (TcpIpConnectionManager) getConnectionManager(instance);
        final NioNetworking networking = (NioNetworking) connectionManager.getNetworking();
        sleepSeconds(2);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(0, networking.getChannels().size());
            }
        });
        instance.shutdown();
    }

}
