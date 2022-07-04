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

package com.hazelcast.spring;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;

import javax.annotation.Resource;

import static org.junit.Assert.assertEquals;

@RunWith(CustomSpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"client-network-defaults-context.xml"})
@Category(QuickTest.class)
public class TestClientConfigAndSpringDefaults {

    private ClientConfig clientConfig;

    @Resource(name = "client")
    private HazelcastClientProxy client;

    @BeforeClass
    @AfterClass
    public static void start() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Before
    public void before() {
        clientConfig = client.getClientConfig();
    }

    @Test
    public void testDefaults() {
        ClientConfig defaults = new ClientConfig();

        assertEquals(defaults.getNetworkConfig().isSmartRouting(), clientConfig.getNetworkConfig().isSmartRouting());
        assertEquals(defaults.getNetworkConfig().getConnectionTimeout(), clientConfig.getNetworkConfig().getConnectionTimeout());
    }
}
