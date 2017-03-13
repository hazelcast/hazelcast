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

package com.hazelcast.client.test.bounce;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.spi.properties.ClientProperty;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.bounce.BounceMemberRule;
import com.hazelcast.test.bounce.BounceTestConfiguration;
import com.hazelcast.test.bounce.DriverFactory;

import java.net.InetSocketAddress;

import static com.hazelcast.test.HazelcastTestSupport.waitAllForSafeState;

/**
 * Default client-side test driver factory for bouncing members tests
 */
public class ClientDriverFactory implements DriverFactory {
    @Override
    public HazelcastInstance[] createTestDrivers(BounceMemberRule rule) {
        BounceTestConfiguration testConfiguration = rule.getBounceTestConfig();
        switch (testConfiguration.getDriverType()) {
            case CLIENT:
                HazelcastInstance[] drivers = new HazelcastInstance[testConfiguration.getDriverCount()];
                for (int i = 0; i < drivers.length; i++) {
                    drivers[i] = ((TestHazelcastFactory) rule.getFactory())
                            .newHazelcastClient(getClientConfig(rule.getSteadyMember()));
                }
                waitAllForSafeState(drivers);
                return drivers;
            default:
                throw new AssertionError(
                        "ClientDriverFactory cannot create test drivers for " + testConfiguration.getDriverType());
        }
    }

    /**
     * Creates a client config for a unisocket client that connects to the given member only
     * (to avoid exception as other members are bouncing).
     */
    protected ClientConfig getClientConfig(HazelcastInstance member) {
        // get client configuration that guarantees our client will connect to the specific member
        ClientConfig config = new ClientConfig();
        config.setProperty(ClientProperty.SHUFFLE_MEMBER_LIST.getName(), "false");
        config.getNetworkConfig().setSmartRouting(false);

        InetSocketAddress socketAddress = member.getCluster().getLocalMember().getSocketAddress();

        config.getNetworkConfig().
                addAddress(socketAddress.getHostName() + ":" + socketAddress.getPort());

        return config;
    }
}
