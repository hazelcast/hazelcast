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

package com.hazelcast.client.bluegreen;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.client.test.ClientTestSupport;
import com.hazelcast.config.Config;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.core.Member;
import com.hazelcast.nio.Address;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class FailoverTest extends ClientTestSupport {

    @After
    public void cleanUp() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    public void testFailover() {
        Config config1 = new Config();
        config1.getGroupConfig().setName("dev1");
        config1.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config1.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(config1);

        Config config2 = new Config();
        config2.getGroupConfig().setName("dev2");
        config2.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config2.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
        HazelcastInstance instance2 = Hazelcast.newHazelcastInstance(config2);

        final ClientConfig clientConfig = new ClientConfig();
        clientConfig.getGroupConfig().setName("dev1");
        ClientNetworkConfig networkConfig = clientConfig.getNetworkConfig();
        Member member1 = (Member) instance1.getLocalEndpoint();
        Address address1 = member1.getAddress();
        networkConfig.setAddresses(Collections.singletonList(address1.getHost() + ":" + address1.getPort()));

        //prepare client config for alternative cluster
        Member member2 = (Member) instance2.getLocalEndpoint();
        Address address2 = member2.getAddress();
        ClientConfig alternativeClientConfig = new ClientConfig();
        alternativeClientConfig.getGroupConfig().setName("dev2");
        alternativeClientConfig.getNetworkConfig().setAddresses(Collections.singletonList(address2.getHost() + ":" + address2.getPort()));

        clientConfig.addAlternativeClientConfig(alternativeClientConfig);

        final CountDownLatch countDownLatch = new CountDownLatch(3);
        clientConfig.addListenerConfig(new ListenerConfig(new LifecycleListener() {
            @Override
            public void stateChanged(LifecycleEvent event) {
                if (LifecycleEvent.LifecycleState.CLIENT_CONNECTED.equals(event.getState())) {
                    countDownLatch.countDown();
                } else if (LifecycleEvent.LifecycleState.CLIENT_DISCONNECTED.equals(event.getState())) {
                    countDownLatch.countDown();
                }
            }
        }));

        final HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);

        Set<Member> members = client.getCluster().getMembers();
        assertEquals(1, members.size());
        assertContains(members, member1);

        instance1.shutdown();

        assertOpenEventually(countDownLatch);

        members = client.getCluster().getMembers();
        assertEquals(1, members.size());
        assertContains(members, member2);
    }
}

