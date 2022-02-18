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

package com.hazelcast.client.connectionstrategy;

import com.hazelcast.client.config.ClientClasspathXmlConfig;
import com.hazelcast.client.test.ClientTestSupport;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;

import static com.hazelcast.core.LifecycleEvent.LifecycleState.CLIENT_CONNECTED;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class})
public class ConfiguredBehaviourTestXmlConfig extends ClientTestSupport {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();
    private static final CountDownLatch asyncStartFromXmlLatch = new CountDownLatch(1);

    private static class AsyncStartListener implements LifecycleListener {
        @Override
        public void stateChanged(LifecycleEvent event) {
            if (event.getState().equals(CLIENT_CONNECTED)) {
                asyncStartFromXmlLatch.countDown();
            }
        }
    }

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    @Test
    public void testAsyncStartTrueXmlConfig() {
        ClientClasspathXmlConfig clientConfig = new ClientClasspathXmlConfig(
                "hazelcast-client-connection-strategy-asyncStart-true.xml");

        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        assertTrue(client.getLifecycleService().isRunning());

        hazelcastFactory.newHazelcastInstance();

        assertOpenEventually(asyncStartFromXmlLatch);

        client.getMap(randomMapName());
    }

    @Test
    public void testReconnectModeASYNCSingleMemberStartLateXmlConfig() {

        HazelcastInstance hazelcastInstance = hazelcastFactory.newHazelcastInstance();

        ClientClasspathXmlConfig clientConfig = new ClientClasspathXmlConfig(
                "hazelcast-client-connection-strategy-asyncReconnect.xml");

        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        assertTrue(client.getLifecycleService().isRunning());

        ReconnectListener reconnectListener = new ReconnectListener();
        client.getLifecycleService().addLifecycleListener(reconnectListener);

        hazelcastInstance.shutdown();
        hazelcastFactory.newHazelcastInstance();

        assertOpenEventually(reconnectListener.reconnectedLatch);

        client.getMap(randomMapName());
    }
}
