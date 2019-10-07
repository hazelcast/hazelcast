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

package com.hazelcast.client.impl.connection.nio;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientConnectionStrategyConfig;
import com.hazelcast.client.config.ConnectionRetryConfig;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.test.ClientTestSupport;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.client.util.ClientStateListener;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cluster.Address;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Iterator;
import java.util.LinkedHashSet;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClusterConnectionRetryTest extends ClientTestSupport {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    @Test
    public void testConnectionExponentialRetryAttempts() throws InterruptedException {
        int retryTimeoutMultiplier = 2;
        int baseRetryTimeoutMillis = 100;
        int capRetryTimeoutMillis = 700;

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().addAddress("127.0.0.1:5701");
        clientConfig.setProperty("hazelcast.client.connection.strategy.classname", RecordingStrategy.class.getName());
        ClientConnectionStrategyConfig connectionStrategyConfig = clientConfig.getConnectionStrategyConfig();
        connectionStrategyConfig.setAsyncStart(true);

        //configure exponential retry
        ConnectionRetryConfig connectionRetryConfig = connectionStrategyConfig.getConnectionRetryConfig();
        connectionRetryConfig.setJitter(0);
        connectionRetryConfig.setFailOnMaxBackoff(true);
        connectionRetryConfig.setMultiplier(retryTimeoutMultiplier);
        connectionRetryConfig.setInitialBackoffMillis(baseRetryTimeoutMillis);
        connectionRetryConfig.setMaxBackoffMillis(capRetryTimeoutMillis);

        ClientStateListener clientStateListener = new ClientStateListener(clientConfig);
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);
        HazelcastClientInstanceImpl clientImpl = getHazelcastClientInstanceImpl(client);
        ClientConnectionManagerImpl connectionManager = (ClientConnectionManagerImpl) clientImpl.getConnectionManager();
        RecordingStrategy connectionStrategy = (RecordingStrategy) connectionManager.getConnectionStrategy();

        clientStateListener.awaitDisconnected();

        LinkedHashSet<Long> attemptTimeStamps = connectionStrategy.getAttemptTimeStamps();
        Iterator<Long> iterator = attemptTimeStamps.iterator();
        Long startPoint = iterator.next();
        Long last = 0L;
        int expectedSleepBetweenAttempts = baseRetryTimeoutMillis;
        while (iterator.hasNext()) {
            Long attemptTimeStamp = iterator.next();
            long actualSleepBetweenAttempts = attemptTimeStamp - startPoint - last;
            assertGreaterOrEquals("sleep between attempts", actualSleepBetweenAttempts, expectedSleepBetweenAttempts);
            expectedSleepBetweenAttempts *= retryTimeoutMultiplier;
            expectedSleepBetweenAttempts = Math.min(capRetryTimeoutMillis, expectedSleepBetweenAttempts);
            last = attemptTimeStamp - startPoint;
        }
    }

    @Test
    public void testConnectionExponentialRetryAttempts_jitterEnabled() throws InterruptedException {
        int retryTimeoutMultiplier = 2;
        int initialBackoffMillis = 100;
        int capRetryTimeoutMillis = 700;

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().addAddress("127.0.0.1:5701");
        clientConfig.setProperty("hazelcast.client.connection.strategy.classname", RecordingStrategy.class.getName());
        ClientConnectionStrategyConfig connectionStrategyConfig = clientConfig.getConnectionStrategyConfig();
        connectionStrategyConfig.setAsyncStart(true);

        //configure exponential retry
        ConnectionRetryConfig connectionRetryConfig = connectionStrategyConfig.getConnectionRetryConfig();
        double jitter = 0.5;
        connectionRetryConfig.setJitter(jitter);
        connectionRetryConfig.setFailOnMaxBackoff(true);
        connectionRetryConfig.setMultiplier(retryTimeoutMultiplier);
        connectionRetryConfig.setInitialBackoffMillis(initialBackoffMillis);
        connectionRetryConfig.setMaxBackoffMillis(capRetryTimeoutMillis);

        ClientStateListener clientStateListener = new ClientStateListener(clientConfig);
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);
        HazelcastClientInstanceImpl clientImpl = getHazelcastClientInstanceImpl(client);
        ClientConnectionManagerImpl connectionManager = (ClientConnectionManagerImpl) clientImpl.getConnectionManager();
        RecordingStrategy connectionStrategy = (RecordingStrategy) connectionManager.getConnectionStrategy();

        clientStateListener.awaitDisconnected();

        LinkedHashSet<Long> attemptTimeStamps = connectionStrategy.getAttemptTimeStamps();
        Iterator<Long> iterator = attemptTimeStamps.iterator();
        Long startPoint = iterator.next();
        Long last = 0L;
        int currentBackoffMillis = initialBackoffMillis;

        while (iterator.hasNext()) {
            long attemptTimeStamp = iterator.next();
            long actualSleepBetweenAttempts = attemptTimeStamp - startPoint - last;
            long lowerBound = (long) (currentBackoffMillis - currentBackoffMillis * jitter);
            assertGreaterOrEquals("sleep between attempts", actualSleepBetweenAttempts, lowerBound);
            currentBackoffMillis *= retryTimeoutMultiplier;
            last = attemptTimeStamp - startPoint;
        }
    }

    public static class RecordingStrategy extends DefaultClientConnectionStrategy {

        private final LinkedHashSet<Long> attemptTimeStamps = new LinkedHashSet<Long>();

        public void beforeOpenConnection(Address target) {
            attemptTimeStamps.add(System.currentTimeMillis());
            super.beforeOpenConnection(target);
        }

        LinkedHashSet<Long> getAttemptTimeStamps() {
            return attemptTimeStamps;
        }
    }


}
