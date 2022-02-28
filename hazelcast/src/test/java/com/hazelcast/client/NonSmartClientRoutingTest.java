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
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static org.junit.Assert.assertEquals;

/**
 * A test that verifies that a non smart client, can send request to a wrong node, but still can get responses to its requests.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class NonSmartClientRoutingTest {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    private HazelcastInstance client;
    private HazelcastInstance server1;

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    @Before
    public void setup() {
        server1 = hazelcastFactory.newHazelcastInstance();
        hazelcastFactory.newHazelcastInstance();
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setSmartRouting(false);
        client = hazelcastFactory.newHazelcastClient(clientConfig);
    }


    @Test
    public void test() {
        String mapName = randomString();
        // create some dummy data.
        Map<String, String> origin = new HashMap<String, String>();
        for (int k = 0; k < 1000; k++) {
            String value = randomString();
            origin.put(value, value);
        }

        // insert that data on the server.
        server1.getMap(mapName).putAll(origin);

        // now iterate over that dummy data and make sure that we find everything that was inserted.
        IMap<String, String> map = client.getMap(mapName);
        for (Map.Entry<String, String> entry : origin.entrySet()) {
            String key = entry.getKey();
            String expectedValue = map.get(key);
            String actualValue = map.get(key);
            assertEquals(expectedValue, actualValue);
        }
    }
}
