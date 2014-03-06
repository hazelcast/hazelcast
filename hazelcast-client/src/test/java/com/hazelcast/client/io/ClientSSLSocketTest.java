/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.io;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.nio.ssl.TestKeyStoreUtil;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

/**
 * @author mdogan 8/23/13
 */

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClientSSLSocketTest {

    @After
    @Before
    public void cleanup() throws Exception {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    public void test() throws IOException {
        Properties props = TestKeyStoreUtil.createSslProperties();
        Config cfg = new Config();
        cfg.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        cfg.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true).addMember("127.0.0.1");
        cfg.getNetworkConfig().setSSLConfig(new SSLConfig().setEnabled(true).setProperties(props));
        final HazelcastInstance hz1 = Hazelcast.newHazelcastInstance(cfg);
        final HazelcastInstance hz2 = Hazelcast.newHazelcastInstance(cfg);

        ClientConfig config = new ClientConfig();
        config.getNetworkConfig().addAddress("127.0.0.1");
        config.getNetworkConfig().setRedoOperation(true);
        config.getNetworkConfig().setSSLConfig(new SSLConfig().setEnabled(true).setProperties(props));


        final HazelcastInstance client = HazelcastClient.newHazelcastClient(config);
        IMap<Object, Object> clientMap = client.getMap("test");

        int size = 1000;
        for (int i = 0; i < size; i++) {
            Assert.assertNull(clientMap.put(i, 2 * i + 1));
        }

        IMap<Object, Object> map = hz1.getMap("test");
        for (int i = 0; i < size; i++) {
            assertEquals(2 * i + 1, map.get(i));
        }
    }
}

