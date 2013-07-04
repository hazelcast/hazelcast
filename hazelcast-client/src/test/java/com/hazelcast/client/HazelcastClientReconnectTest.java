/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.config.Config;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.test.HazelcastJUnit4ClassRunner;
import com.hazelcast.test.annotation.SerialTest;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * @author Dmitry Shohov
 */
@RunWith(HazelcastJUnit4ClassRunner.class)
@Category(SerialTest.class)
public class HazelcastClientReconnectTest {

    @BeforeClass
    public static void init() throws Exception {
        System.setProperty(GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "1");
        System.setProperty(GroupProperties.PROP_VERSION_CHECK_ENABLED, "false");
        Hazelcast.shutdownAll();
    }

    @After
    @Before
    public void cleanup() throws Exception {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test(timeout = 180000)
    @Ignore
    public void testClientReconnectOnClusterDown() throws Exception {
        Config config = new Config();
        final HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setGroupConfig(new GroupConfig(h1.getConfig().getGroupConfig().getName(), h1.getConfig().getGroupConfig().getPassword()));
        clientConfig.setConnectionAttemptLimit(Integer.MAX_VALUE);
        final HazelcastInstance client1 = HazelcastClient.newHazelcastClient(clientConfig);

        h1.getLifecycleService().shutdown();
        //wait enough time to GET_MEMBERS command to appear and to default connectionTimeout (30 seconds) pass
        Thread.sleep(50000);

        //creating this thread to wait for put operation to complete
        //this will block test until client is connected
        Thread thread = new Thread(new Runnable() {
            public void run() {
                IMap<String, String> m = client1.getMap("default");
                m.put("test", "test");
            }
        });
        thread.start();

        //now create server
        Hazelcast.newHazelcastInstance(config);
        //now we have situation when OutRunnable is blocked in checkOnReconnect,
        // but ConnectionManager.heartbeatTimer will kill newly opened connection after 3 seconds (connectionTimeout / 10)

        //block test until put operation succeeds, which means client is reconnected
        thread.join();
    }

    @Test(timeout = 180000)
    @Ignore
    public void testClientReconnectOnClusterDownWithEntryListeners() throws Exception {
        Config config = new Config();
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setGroupConfig(new GroupConfig(h1.getConfig().getGroupConfig().getName(), h1.getConfig().getGroupConfig().getPassword()));
        clientConfig.setConnectionAttemptLimit(Integer.MAX_VALUE);
        //this test depends on connection timeout and adapted to run with 30000 value, which is also default
        clientConfig.setConnectionTimeout(30000);
        final HazelcastInstance client1 = HazelcastClient.newHazelcastClient(clientConfig);
        //add 35 listeners to make sure OutRunnable.checkOnReconnect will spend 35 * 100 ms
        //this will make it impossible to send command in 3 sec window before ConnecitonManager.hearbeatTimer will terminate the connection
        //in real situation with 1 or 2 listeners OutRunnable.resubscribe will increase OutRunnable.reconnectionCalls list on every unsuccessful reconnect
        // meaning every unsuccessful reconnect will lower the chance for reconnect to succeed.
        // If server or connection to the server is slow then a few failed initial reconnects will make it impossible to reconnect in future
        for (int i = 0; i < 35; i++) {
            final IMap<String, String> m = client1.getMap("default" + i);
            m.addEntryListener(new EntryAdapter<String, String>(), true);
        }

        h1.getLifecycleService().shutdown();
        //wait enough time to GET_MEMBERS command to appear and to default connectionTimeout (30 seconds) pass
        Thread.sleep(50000);

        //creating this thread to wait for put operation to complete
        //this will block test until client is connected
        Thread thread = new Thread(new Runnable() {
            public void run() {
                IMap<String, String> m = client1.getMap("default");
                m.put("test", "test");
            }
        });
        thread.start();

        //now create server
        System.out.println("Creating server");
        final HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        //now we have situation when OutRunnable is blocked in checkOnReconnect,
        // but ConnectionManager.heartbeatTimer will kill newly opened connection after 3 seconds (connectionTimeout / 10)

        //block test until put operation succeeds, which means client is reconnected
        thread.join();
    }
}