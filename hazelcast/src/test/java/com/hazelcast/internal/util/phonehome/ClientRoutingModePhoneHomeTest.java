/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.util.phonehome;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestAwareClientFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;

import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.SMART_CLIENTS_COUNT;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.UNISOCKET_CLIENTS_COUNT;
import static com.hazelcast.test.Accessors.getNode;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientRoutingModePhoneHomeTest extends HazelcastTestSupport {

    private final TestAwareClientFactory factory = new TestAwareClientFactory();
    private HazelcastInstance instance;
    private PhoneHome phoneHome;
    private Map<String, String> parameters;


    @Before
    public void initialise() {
        instance = factory.newHazelcastInstance(new Config());
        Node node = getNode(instance);
        phoneHome = new PhoneHome(node);
    }

    @After
    public void after() {
        factory.terminateAll();
    }

    private void refreshMetrics() {
        parameters = phoneHome.phoneHome(true);
    }

    private void newPhoneHome(HazelcastInstance instance) {
        phoneHome = new PhoneHome(getNode(instance));
        refreshMetrics();
    }

    @Test
    public void testMultipleClientsRegistered() {
        ClientConfig smartClient = new ClientConfig();
        smartClient.getNetworkConfig().setSmartRouting(true);
        ClientConfig unisocketClient = new ClientConfig();
        unisocketClient.getNetworkConfig().setSmartRouting(false);

        newPhoneHome(instance);
        assertThat(get(SMART_CLIENTS_COUNT)).isEqualTo("0");
        assertThat(get(SMART_CLIENTS_COUNT)).isEqualTo("0");

        factory.newHazelcastClient(smartClient);
        factory.newHazelcastClient(unisocketClient);

        newPhoneHome(instance);

        assertThat(get(UNISOCKET_CLIENTS_COUNT)).isEqualTo("1");
        assertThat(get(SMART_CLIENTS_COUNT)).isEqualTo("1");

        factory.newHazelcastClient(smartClient);
        factory.newHazelcastClient(smartClient);
        factory.newHazelcastClient(unisocketClient);
        factory.newHazelcastClient(unisocketClient);

        newPhoneHome(instance);
        assertThat(get(UNISOCKET_CLIENTS_COUNT)).isEqualTo("3");
        assertThat(get(SMART_CLIENTS_COUNT)).isEqualTo("3");
    }

    @Test
    public void testMultipleClientsRemoved() {
        ClientConfig smartClient = new ClientConfig();
        smartClient.getNetworkConfig().setSmartRouting(true);
        ClientConfig unisocketClient = new ClientConfig();
        unisocketClient.getNetworkConfig().setSmartRouting(false);

        HazelcastInstance client1 = factory.newHazelcastClient(smartClient);
        HazelcastInstance client2 = factory.newHazelcastClient(unisocketClient);

        newPhoneHome(instance);
        assertThat(get(UNISOCKET_CLIENTS_COUNT)).isEqualTo("1");
        assertThat(get(SMART_CLIENTS_COUNT)).isEqualTo("1");

        // terminate SMART client
        client1.shutdown();

        newPhoneHome(instance);
        assertThat(get(UNISOCKET_CLIENTS_COUNT)).isEqualTo("1");
        assertThat(get(SMART_CLIENTS_COUNT)).isEqualTo("0");

        // terminate UNISOCKET client
        client2.shutdown();

        newPhoneHome(instance);
        assertThat(get(UNISOCKET_CLIENTS_COUNT)).isEqualTo("0");
    }

    private String get(Metric metric) {
        return parameters.get(metric.getQueryParameter());
    }
}
