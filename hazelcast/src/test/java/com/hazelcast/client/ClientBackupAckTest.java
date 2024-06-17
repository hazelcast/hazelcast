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

package com.hazelcast.client;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.connection.tcp.RoutingMode;
import com.hazelcast.client.impl.spi.EventHandler;
import com.hazelcast.client.impl.spi.impl.ClientInvocationServiceImpl;
import com.hazelcast.client.test.ClientTestSupport;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.client.util.ConfigRoutingUtil;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Assume;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static com.hazelcast.client.impl.connection.tcp.RoutingMode.SMART;
import static com.hazelcast.client.impl.connection.tcp.RoutingMode.SUBSET;
import static com.hazelcast.client.impl.connection.tcp.RoutingMode.UNISOCKET;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

@RunWith(HazelcastParametrizedRunner.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientBackupAckTest extends ClientTestSupport {

    @Parameterized.Parameter
    public RoutingMode routingMode;

    @Parameterized.Parameters(name = "{index}: routingMode={0}")
    public static Iterable<?> parameters() {
        return Arrays.asList(UNISOCKET, RoutingMode.SMART);
    }

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @After
    public void cleanup() {
        hazelcastFactory.terminateAll();
    }

    @Test
    public void testBackupAckToClientIsEnabled_byDefault() {
        Assume.assumeTrue(routingMode == RoutingMode.SMART);

        hazelcastFactory.newHazelcastInstance();

        ClientConfig clientConfig = newClientConfig();
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        assertTrue(isEnabled(client));
    }

    @Test
    public void testBackupAckToClientIsEnabled() {
        hazelcastFactory.newHazelcastInstance();

        ClientConfig clientConfig = newClientConfig();
        clientConfig.setBackupAckToClientEnabled(true);
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        boolean backupAckToClientEnabled
                = getHazelcastClientInstanceImpl(client)
                .getInvocationService().isBackupAckToClientEnabled();

        if (routingMode == UNISOCKET || routingMode == SUBSET) {
            assertFalse(isEnabled(client));
            assertFalse(backupAckToClientEnabled);
        } else if (routingMode == SMART) {
            assertTrue(isEnabled(client));
            assertTrue(backupAckToClientEnabled);
        } else {
            throw new IllegalStateException("Unexpected value: " + routingMode);
        }
    }

    @Test
    public void testBackupAckToClientIsDisabled() {
        hazelcastFactory.newHazelcastInstance();

        ClientConfig clientConfig = newClientConfig();
        clientConfig.setBackupAckToClientEnabled(false);
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        assertFalse(isEnabled(client));
    }

    private boolean isEnabled(HazelcastInstance client) {
        Collection<EventHandler> values = getAllEventHandlers(client).values();
        for (EventHandler value : values) {
            if (value instanceof ClientInvocationServiceImpl.BackupEventHandler) {
                return true;
            }
        }
        return false;
    }

    private ClientConfig newClientConfig() {
        ClientConfig clientConfig = ConfigRoutingUtil.newClientConfig(routingMode);
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE);
        return clientConfig;
    }
}
