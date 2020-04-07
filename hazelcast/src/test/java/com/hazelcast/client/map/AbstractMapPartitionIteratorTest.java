/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.After;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.Iterator;
import java.util.Map;

import static java.util.Arrays.asList;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

public abstract class AbstractMapPartitionIteratorTest extends HazelcastTestSupport {

    @Parameter
    public boolean prefetchValues;

    @Parameters(name = "prefetchValues:{0}")
    public static Iterable<Object[]> parameters() {
        return asList(new Object[]{Boolean.TRUE}, new Object[]{Boolean.FALSE});
    }

    protected TestHazelcastFactory factory;
    protected HazelcastInstance server;
    protected HazelcastInstance client;

    @After
    public void teardown() {
        factory.terminateAll();
    }

    @Test
    public void test_HasNext_Returns_False_On_EmptyPartition() throws Exception {
        ClientMapProxy<Integer, Integer> map = getMapProxy();

        Iterator<Map.Entry<Integer, Integer>> iterator = map.iterator(10, 1, prefetchValues);
        assertFalse(iterator.hasNext());
    }

    @Test
    public void test_HasNext_Returns_True_On_NonEmptyPartition() throws Exception {
        ClientMapProxy<String, String> map = getMapProxy();

        String key = generateKeyForPartition(server, 1);
        String value = randomString();
        map.put(key, value);

        Iterator<Map.Entry<String, String>> iterator = map.iterator(10, 1, prefetchValues);
        assertTrue(iterator.hasNext());
    }

    @Test
    public void test_Next_Returns_Value_On_NonEmptyPartition() throws Exception {
        ClientMapProxy<String, String> map = getMapProxy();

        String key = generateKeyForPartition(server, 1);
        String value = randomString();
        map.put(key, value);

        Iterator<Map.Entry<String, String>> iterator = map.iterator(10, 1, prefetchValues);
        Map.Entry entry = iterator.next();
        assertEquals(value, entry.getValue());
    }

    @Test
    public void test_Next_Returns_Values_When_FetchSizeExceeds_On_NonEmptyPartition() throws Exception {
        ClientMapProxy<String, String> map = getMapProxy();
        String value = randomString();
        int count = 1000;
        for (int i = 0; i < count; i++) {
            String key = generateKeyForPartition(server, 42);
            map.put(key, value);
        }
        Iterator<Map.Entry<String, String>> iterator = map.iterator(10, 42, prefetchValues);
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
