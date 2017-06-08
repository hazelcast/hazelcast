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

package com.hazelcast.client.connectionstrategy;

import com.hazelcast.client.HazelcastClientOfflineException;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.ClientTestSupport;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.client.config.ClientConnectionStrategyConfig.ReconnectMode.OFF;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ConfiguredBehaviourTest extends ClientTestSupport {
    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @Test
    public void testDefaultStartMode() {
        hazelcastFactory.newHazelcastInstance();

        HazelcastInstance client = hazelcastFactory.newHazelcastClient();

        // Since default start mode is synch mode, this should not throw exception
        client.getMap(randomMapName());
    }

    @Test(expected = HazelcastClientOfflineException.class)
    public void testAsyncStartMode() {
        hazelcastFactory.newHazelcastInstance();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().setClientStartAsync(true);

        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        client.getMap(randomMapName());
    }

    @Test(expected = HazelcastClientOfflineException.class)
    public void testReconnectModeOFFSingleMember() {
        HazelcastInstance hazelcastInstance = hazelcastFactory.newHazelcastInstance();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().setReconnectMode(OFF);
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        // no exception at this point
        IMap<Integer, Integer> map = client.getMap(randomMapName());
        map.put(1, 5);

        hazelcastInstance.shutdown();

        map.put(1, 5);
    }

    @Test(expected = HazelcastClientOfflineException.class)
    public void testReconnectModeOFFTwoMembers() {
        HazelcastInstance[] hazelcastInstances = hazelcastFactory.newInstances(getConfig(), 2);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().setReconnectMode(OFF);
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        // no exception at this point
        IMap<Integer, Integer> map = client.getMap(randomMapName());
        map.put(1, 5);

        hazelcastInstances[0].shutdown();

        map.put(1, 5);
    }
}
