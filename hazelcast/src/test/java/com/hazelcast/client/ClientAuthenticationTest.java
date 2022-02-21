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
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.instance.impl.DefaultNodeExtension;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.instance.impl.NodeExtension;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.security.SimpleTokenCredentials;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.mocknetwork.MockNodeContext;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import static com.hazelcast.client.impl.management.ManagementCenterService.MC_CLIENT_MODE_PROP;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientAuthenticationTest extends HazelcastTestSupport {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @After
    public void cleanup() {
        hazelcastFactory.terminateAll();
    }

    @Test(expected = IllegalStateException.class)
    public void testNoClusterFound() {
        final ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(2000);
        hazelcastFactory.newHazelcastClient(clientConfig);
    }

    @Test
    public void testAuthenticationWithCustomCredentials_when_singleNode() {
        DataSerializableFactory factory = new CustomCredentialsIdentifiedFactory();

        // with this config, the server will authenticate any credential of type CustomCredentials
        Config config = new Config();
        config.getSerializationConfig().addDataSerializableFactory(1, factory);
        hazelcastFactory.newHazelcastInstance(config);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getSecurityConfig().setCredentials(new CustomCredentials());

        // custom credentials are not supported when security is disabled on members
        expectedException.expect(IllegalStateException.class);

        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(1000);
        hazelcastFactory.newHazelcastClient(clientConfig);
    }

    @Test
    public void testAuthentication_with_mcModeEnabled() {
        hazelcastFactory.newHazelcastInstance();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setProperty(MC_CLIENT_MODE_PROP.getName(), "true");
        // if the client is able to connect, it's a pass
        hazelcastFactory.newHazelcastClient(clientConfig);
    }

    @Test
    public void testAuthentication_with_mcModeEnabled_when_clusterStart_isNotComplete() {
        HazelcastInstanceFactory.newHazelcastInstance(new Config(), randomName(),
                new MockNodeContext(hazelcastFactory.getRegistry(), hazelcastFactory.nextAddress()) {
                    @Override
                    public NodeExtension createNodeExtension(Node node) {
                        return new DefaultNodeExtension(node) {
                            @Override
                            public boolean isStartCompleted() {
                                return false;
                            }
                        };
                    }
                });

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setProperty(MC_CLIENT_MODE_PROP.getName(), "true");
        // if the client is able to connect, it's a pass
        hazelcastFactory.newHazelcastClient(clientConfig);
    }

    private static class CustomCredentialsIdentifiedFactory implements DataSerializableFactory {
        @Override
        public IdentifiedDataSerializable create(int classId) {
            return new CustomCredentials();
        }
    }

    private static class CustomCredentials extends SimpleTokenCredentials {
        @Override
        public byte[] getToken() {
            return new byte[10];
        }
    }
}
