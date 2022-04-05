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

package com.hazelcast.client;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;

import static com.hazelcast.test.HazelcastTestSupport.assertContains;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class})
public class ClientOutBoundPortTest {

    @Before
    @After
    public void cleanUp() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    public void clientOutboundPortRangeTest() {
        Config config1 = new Config();
        config1.setClusterName("client-out-test");
        Hazelcast.newHazelcastInstance(config1);

        ClientConfig config2 = new ClientConfig();
        config2.setClusterName("client-out-test");
        config2.getNetworkConfig().setOutboundPortDefinitions(Arrays.asList("34700", "34703-34705"));
        HazelcastInstance client = HazelcastClient.newHazelcastClient(config2);

        final int port = ((Client) client.getLocalEndpoint()).getSocketAddress().getPort();

        assertContains(Arrays.asList(34700, 34703, 34704, 34705), port);
    }

}
