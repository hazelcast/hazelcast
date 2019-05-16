/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.nio.tcp.TcpIpNetworkingService;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Set;

import static com.hazelcast.core.Hazelcast.newHazelcastInstance;
import static com.hazelcast.spi.properties.GroupProperty.MERGE_FIRST_RUN_DELAY_SECONDS;
import static com.hazelcast.spi.properties.GroupProperty.MERGE_NEXT_RUN_DELAY_SECONDS;
import static com.hazelcast.spi.properties.GroupProperty.WAIT_SECONDS_BEFORE_JOIN;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class NioChannelMemoryLeakTest extends HazelcastTestSupport {

    @After
    public void cleanUp() {
        Hazelcast.shutdownAll();
    }

    @Test
    public void testNioChannelLeak() {
        Config config = getConfig();
        config.setProperty(MERGE_NEXT_RUN_DELAY_SECONDS.getName(), "1");
        config.setProperty(MERGE_FIRST_RUN_DELAY_SECONDS.getName(), "1");

        HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
        TcpIpNetworkingService networkingService = (TcpIpNetworkingService) getNode(instance).getNetworkingService();
        final NioNetworking networking = (NioNetworking) networkingService.getNetworking();

        assertTrueEventually(() -> assertThat(networking.getChannels(), Matchers.empty()));
    }

    @Test
    public void testNioChannelLeak_afterMultipleSplitBrainMerges() {
        Config config = getConfig();
        config.setProperty(WAIT_SECONDS_BEFORE_JOIN.getName(), "0");
        config.setProperty(MERGE_NEXT_RUN_DELAY_SECONDS.getName(), "99999999");
        config.setProperty(MERGE_FIRST_RUN_DELAY_SECONDS.getName(), "99999999");

        final HazelcastInstance instance1 = newHazelcastInstance(config);
        final HazelcastInstance instance2 = newHazelcastInstance(config);
        final HazelcastInstance instance3 = newHazelcastInstance(config);
        assertClusterSizeEventually(3, instance1, instance2, instance3);

        for (int i = 0; i < 5; i++) {
            closeConnectionBetween(instance1,  instance3);
            closeConnectionBetween(instance2,  instance3);
            assertClusterSizeEventually(2, instance1, instance2);
            assertClusterSizeEventually(1, instance3);

            getNode(instance3).getClusterService().merge(getAddress(instance1));
            assertClusterSizeEventually(3, instance1, instance2, instance3);
        }

        assertTrueEventually(() -> {
            assertNoChannelLeak(instance1);
            assertNoChannelLeak(instance2);
            assertNoChannelLeak(instance3);
        });
    }

    private void assertNoChannelLeak(HazelcastInstance instance) {
        int clusterSize = instance.getCluster().getMembers().size();
        // There may be one or two channels between two members.
        // Ideally there'll be only a single channel,
        // but when two members initiate the connection concurrently,
        // it may end up with two different connections between them.
        int maxChannelCount = (clusterSize - 1) * 2;

        final NioNetworking networking = (NioNetworking) getNode(instance).getNetworkingService().getNetworking();
        Set<NioChannel> channels = networking.getChannels();

        assertThat(channels.size(), lessThanOrEqualTo(maxChannelCount));
        for (NioChannel channel : channels) {
            assertTrue("Channel " + channel + " was found closed in instance " + instance,
                    channel.socketChannel().isOpen());
        }
    }

    @Override
    protected Config getConfig() {
        Config config = new Config();
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getTcpIpConfig().addMember("127.0.0.1").setEnabled(true);
        join.getMulticastConfig().setEnabled(false);
        return config;
    }
}
