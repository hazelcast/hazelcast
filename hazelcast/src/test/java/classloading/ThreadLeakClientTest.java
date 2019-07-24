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

package classloading;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientUserCodeDeploymentConfig;
import com.hazelcast.client.impl.spi.impl.discovery.ClientDiscoverySpiTest;
import com.hazelcast.config.DiscoveryStrategyConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.Set;

import static classloading.ThreadLeakTestUtils.assertHazelcastThreadShutdown;
import static classloading.ThreadLeakTestUtils.getThreads;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ThreadLeakClientTest {

    @After
    public void shutdownInstances() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    public void testThreadLeak() {
        Set<Thread> testStartThreads = getThreads();
        HazelcastInstance member = Hazelcast.newHazelcastInstance();
        HazelcastInstance client = HazelcastClient.newHazelcastClient();

        client.shutdown();
        member.shutdown();

        assertHazelcastThreadShutdown(testStartThreads);
    }

    @Test
    public void testThreadLeak_withoutCluster() {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().setAsyncStart(true);
        clientConfig.getNetworkConfig().setConnectionAttemptLimit(Integer.MAX_VALUE);

        Set<Thread> testStartThreads = getThreads();
        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
        client.shutdown();
        assertHazelcastThreadShutdown(testStartThreads);
    }

    @Test(expected = IllegalStateException.class)
    public void testThreadLeakWhenClientCanNotStart() {
        Set<Thread> testStartThreads = getThreads();
        try {
            HazelcastClient.newHazelcastClient();
        } finally {
            assertHazelcastThreadShutdown(testStartThreads);
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testThreadLeakWhenClientCanNotStartDueToAuthenticationError() {
        Hazelcast.newHazelcastInstance();
        ClientConfig config = new ClientConfig();
        config.getGroupConfig().setName("invalid cluster");
        Set<Thread> testStartThreads = getThreads();
        try {
            HazelcastClient.newHazelcastClient(config);
        } finally {
            Hazelcast.shutdownAll();
            assertHazelcastThreadShutdown(testStartThreads);
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testThreadLeakWhenClientCanNotConstructDueToNoMemberDiscoveryStrategyConfig() {
        ClientConfig config = new ClientConfig();
        config.getNetworkConfig().getDiscoveryConfig().addDiscoveryStrategyConfig(
                new DiscoveryStrategyConfig(new ClientDiscoverySpiTest.NoMemberDiscoveryStrategyFactory(),
                        Collections.<String, Comparable>emptyMap()));
        Set<Thread> testStartThreads = getThreads();
        try {
            HazelcastClient.newHazelcastClient(config);
        } finally {
            assertHazelcastThreadShutdown(testStartThreads);
        }
    }

    @Test(expected = HazelcastException.class)
    public void testThreadLeakWhenClientCanNotStartDueToIncorrectUserCodeDeploymentClass() {
        Hazelcast.newHazelcastInstance();

        ClientConfig config = new ClientConfig();
        ClientUserCodeDeploymentConfig clientUserCodeDeploymentConfig = new ClientUserCodeDeploymentConfig();
        clientUserCodeDeploymentConfig.addClass("invalid.class.test");
        config.setUserCodeDeploymentConfig(clientUserCodeDeploymentConfig.setEnabled(true));
        Set<Thread> testStartThreads = getThreads();
        try {
            HazelcastClient.newHazelcastClient(config);
        } finally {
            Hazelcast.shutdownAll();
            assertHazelcastThreadShutdown(testStartThreads);
        }
    }

    @Test(expected = HazelcastSerializationException.class)
    public void testThreadLeakWhenClientCanNotConstructDueToIncorrectSerializationServiceFactoryClassName() {
        ClientConfig config = new ClientConfig();
        SerializationConfig serializationConfig = new SerializationConfig();
        serializationConfig.addDataSerializableFactoryClass(5, "invalid.factory");
        config.setSerializationConfig(serializationConfig);
        Set<Thread> testStartThreads = getThreads();
        try {
            HazelcastClient.newHazelcastClient(config);
        } finally {
            assertHazelcastThreadShutdown(testStartThreads);
        }
    }
}
