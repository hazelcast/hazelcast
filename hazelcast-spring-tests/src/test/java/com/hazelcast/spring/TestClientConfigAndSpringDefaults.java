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

package com.hazelcast.spring;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.core.Hazelcast;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;


@ExtendWith({SpringExtension.class, CustomSpringExtension.class})
@ContextConfiguration(locations = {"client-network-defaults-context.xml"})
class TestClientConfigAndSpringDefaults {

    private ClientConfig clientConfig;

    @Autowired
    private HazelcastClientProxy client;

    @BeforeAll
    @AfterAll
    public static void start() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @BeforeEach
    public void before() {
        clientConfig = client.getClientConfig();
    }

    @Test
    void testDefaults() {
        ClientConfig defaults = new ClientConfig();

        assertEquals(defaults.getNetworkConfig().getConnectionTimeout(), clientConfig.getNetworkConfig().getConnectionTimeout());
        assertEquals(defaults.getNetworkConfig().getClusterRoutingConfig().getRoutingMode(),
                clientConfig.getNetworkConfig().getClusterRoutingConfig().getRoutingMode());
    }
}
