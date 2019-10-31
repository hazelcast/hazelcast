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

package com.hazelcast.client;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.security.SimpleTokenCredentials;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

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
    public void testNoClusterFound() throws Exception {
        final ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setMaxBackoffMillis(2000);
        hazelcastFactory.newHazelcastClient(clientConfig);

    }

    @Test
    public void testAuthenticationWithCustomCredentials_when_singleNode() {
        PortableFactory factory = new CustomCredentialsPortableFactory();

        // with this config, the server will authenticate any credential of type CustomCredentials
        Config config = new Config();
        config.getSerializationConfig()
                .addPortableFactory(1, factory);
        hazelcastFactory.newHazelcastInstance(config);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getSecurityConfig().setCredentials(new CustomCredentials());

        // custom credentials are not supported when security is disabled on members
        expectedException.expect(IllegalStateException.class);
        hazelcastFactory.newHazelcastClient(clientConfig);
    }

    private class CustomCredentialsPortableFactory implements PortableFactory {
        @Override
        public Portable create(int classId) {
            return new CustomCredentials();
        }
    }

    private class CustomCredentials extends SimpleTokenCredentials {
        @Override
        public byte[] getToken() {
            return new byte[10];
        }

    }
}
