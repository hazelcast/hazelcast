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

package com.hazelcast.client.map;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.SlowTest;
import com.hazelcast.test.starter.HazelcastStarter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import testsubjects.StaticSerializableBiFunction;
import testsubjects.StaticSerializableBiFunctionEx;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class ClientMapReplaceAllTest extends HazelcastTestSupport {

    private HazelcastInstance memberInstance;

    protected HazelcastInstance clientInstance;

    @After
    public final void stopHazelcastInstances() {
        if (memberInstance != null) {
            memberInstance.shutdown();
        }
        HazelcastClient.shutdownAll();
    }

    @Before
    public final void startHazelcastInstances() {
        Config config = getConfig();
        MapConfig mapConfig = new MapConfig("mapConfig");
        config.addMapConfig(mapConfig);
        ClientConfig clientConfig = getClientConfig();
        memberInstance = HazelcastStarter.newHazelcastInstance("4.0.3");
        clientInstance = HazelcastClient.newHazelcastClient();
    }

    @Test
    public void testReplaceAllWithStaticSerializableFunction() {
        IMap<String, String> map = clientInstance.getMap(UuidUtil.newUnsecureUuidString());
        map.put("k1", "v1");
        map.put("k2", "v2");
        map.replaceAll(new StaticSerializableBiFunction("v_new"));
        assertEquals(map.get("k1"), "v_new");
        assertEquals(map.get("k2"), "v_new");
    }

    @Test
    public void testReplaceAllWithLambdaFunction() {
        IMap<String, Integer> map = clientInstance.getMap(UuidUtil.newUnsecureUuidString());
        map.put("k1", 1);
        map.put("k2", 2);
        map.replaceAll((k, v) -> v * 10);
        assertEquals((int) map.get("k1"), 10);
        assertEquals((int) map.get("k2"), 20);
    }

    @Test(expected = ArithmeticException.class)
    public void testReplaceAllWithStaticSerializableFunction_ThrowsException() {
        IMap<Integer, Integer> map = clientInstance.getMap(randomString());
        map.put(1, 0);
        map.replaceAll(new StaticSerializableBiFunctionEx());
    }

    protected ClientConfig getClientConfig() {
        return new ClientConfig();
    }
}
