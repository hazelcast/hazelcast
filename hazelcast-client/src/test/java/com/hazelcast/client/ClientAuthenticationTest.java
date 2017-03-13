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

package com.hazelcast.client;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.security.UsernamePasswordCredentials;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientAuthenticationTest extends HazelcastTestSupport {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    private final String USERNAME = "user";
    private final String PASSWORD = "pass";

    @After
    public void cleanup() {
        hazelcastFactory.terminateAll();
    }


    @Test(expected = IllegalStateException.class)
    public void testFailedAuthentication() throws Exception {
        hazelcastFactory.newHazelcastInstance();
        final ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setConnectionAttemptPeriod(1);
        clientConfig.getGroupConfig().setPassword("InvalidPassword");
        hazelcastFactory.newHazelcastClient(clientConfig);
    }

    @Test(expected = IllegalStateException.class)
    public void testNoClusterFound() throws Exception {
        final ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setConnectionAttemptPeriod(1);
        hazelcastFactory.newHazelcastClient(clientConfig);

    }

    @Test
    public void testAuthenticationWithCustomCredentials_when_singleNode() {
        PortableFactory factory = new CustomCredentialsPortableFactory();

        // with this config, the server will authenticate any credential of type CustomCredentials
        Config config = new Config();
        config.getGroupConfig()
                .setName(USERNAME)
                .setPassword(PASSWORD);
        config.getSerializationConfig()
                .addPortableFactory(1, factory);
        hazelcastFactory.newHazelcastInstance(config);

        ClientConfig clientConfig = new ClientConfig();

        // make sure there are no credentials sent over the wire
        clientConfig.getSecurityConfig().setCredentials(new CustomCredentials());
        hazelcastFactory.newHazelcastClient(clientConfig);
    }

    @Test
    public void testAuthenticationWithCustomCredentials_when_multipleNodes() {
        PortableFactory factory = new CustomCredentialsPortableFactory();

        // with this config, the server will authenticate any credential of type CustomCredentials
        Config config = new Config();
        config.getGroupConfig()
                .setName(USERNAME)
                .setPassword(PASSWORD);
        config.getSerializationConfig()
                .addPortableFactory(1, factory);

        hazelcastFactory.newHazelcastInstance(config);
        hazelcastFactory.newHazelcastInstance(config);

        ClientConfig clientConfig = new ClientConfig();

        // make sure there are no credentials sent over the wire
        clientConfig.getSecurityConfig().setCredentials(new CustomCredentials());
        HazelcastInstance hazelcastInstance = hazelcastFactory.newHazelcastClient(clientConfig);

        // ensure client opens a connection to all nodes
        IMap<Integer, Integer> map = hazelcastInstance.getMap(randomName());
        for (int i = 0; i < 100; i++) {
            map.put(i, i);
        }
    }

    private class CustomCredentialsPortableFactory implements PortableFactory {
        @Override
        public Portable create(int classId) {
            return new CustomCredentials() {
                @Override
                public String getUsername() {
                    return USERNAME;
                }

                @Override
                public String getPassword() {
                    return PASSWORD;
                }
            };
        }
    }

    private class CustomCredentials extends UsernamePasswordCredentials {

        @Override
        public int getFactoryId() {
            return 1;
        }

        @Override
        public int getClassId() {
            return 1;
        }
    }
}
