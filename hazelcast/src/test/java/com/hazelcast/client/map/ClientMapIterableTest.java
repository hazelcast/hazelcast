/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.proxy.ClientMapProxy;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.Iterator;
import java.util.Map;

import static java.util.Arrays.asList;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientMapIterableTest extends HazelcastTestSupport {

    @Parameter
    public boolean prefetchValues;

    @Parameters(name = "prefetchValues:{0}")
    public static Iterable<Object[]> parameters() {
        return asList(new Object[] {Boolean.TRUE}, new Object[] {Boolean.FALSE});
    }

    protected TestHazelcastFactory factory;
    protected HazelcastInstance server;
    protected HazelcastInstance client;

    @Before
    public void setup() {
        Config config = getConfig();
        ClientConfig clientConfig = getClientConfig();

        factory = new TestHazelcastFactory();
        server = factory.newHazelcastInstance(config);
        client = factory.newHazelcastClient(clientConfig);
    }

    @After
    public void teardown() {
        factory.terminateAll();
    }

    @Test
    public void test_HasNext_Returns_False_On_EmptyMap() throws Exception {
        ClientMapProxy<Integer, Integer> map = getMapProxy();

        Iterator<Map.Entry<Integer, Integer>> iterator = map.iterable(10, prefetchValues).iterator();
        assertFalse(iterator.hasNext());
    }

    @Test
    public void test_HasNext_Returns_True_On_NonEmptyMap() throws Exception {
        ClientMapProxy<String, String> map = getMapProxy();

        String key = generateKeyForPartition(server, 1);
        String value = randomString();
        map.put(key, value);

        Iterator<Map.Entry<String, String>> iterator = map.iterable(10, prefetchValues).iterator();
        assertTrue(iterator.hasNext());
    }

    @Test
    public void test_Next_Returns_Value_On_NonEmptyMap() throws Exception {
        ClientMapProxy<String, String> map = getMapProxy();

        String key = generateKeyForPartition(server, 1);
        String value = randomString();
        map.put(key, value);

        Iterator<Map.Entry<String, String>> iterator = map.iterable(10, prefetchValues).iterator();
        Map.Entry entry = iterator.next();
        assertEquals(value, entry.getValue());
    }

    @Test
    public void test_Next_Returns_Values_When_FetchSizeExceeds_On_NonEmptyMap() throws Exception {
        ClientMapProxy<String, String> map = getMapProxy();
        String value = randomString();
        int count = 1000;
        for (int i = 0; i < count; i++) {
            String key = generateKeyForPartition(server, 42);
            map.put(key, value);
        }
        Iterator<Map.Entry<String, String>> iterator = map.iterable(10, prefetchValues).iterator();
        for (int i = 0; i < count; i++) {
            Map.Entry entry = iterator.next();
            assertEquals(value, entry.getValue());
        }
    }

    protected ClientConfig getClientConfig() {
        return new ClientConfig();
    }

    private <K, V> ClientMapProxy<K, V> getMapProxy() {
        String mapName = randomString();
        return (ClientMapProxy<K, V>) client.getMap(mapName);
    }
}
