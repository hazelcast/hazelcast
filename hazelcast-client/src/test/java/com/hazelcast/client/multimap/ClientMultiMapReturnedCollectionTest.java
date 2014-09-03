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

package com.hazelcast.client.multimap;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.config.Config;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MultiMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class ClientMultiMapReturnedCollectionTest {

    static HazelcastInstance server;
    static HazelcastInstance client;

    private static final String SET_MAP = "set-map";
    private static final String LIST_MAP = "list-map";

    @BeforeClass
    public static void init() {
        Config config = new Config();
        config.getMultiMapConfig(SET_MAP).setValueCollectionType(MultiMapConfig.ValueCollectionType.SET);
        config.getMultiMapConfig(LIST_MAP).setValueCollectionType(MultiMapConfig.ValueCollectionType.LIST);

        server = Hazelcast.newHazelcastInstance(config);
        client = HazelcastClient.newHazelcastClient();
    }

    @AfterClass
    public static void destroy() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }


    @Test
    public void testGet_withSetBackedValueCollection() throws Exception {
        MultiMap<Integer, Integer> multiMap = client.getMultiMap(SET_MAP);

        multiMap.put(0, 1);
        multiMap.put(0, 2);
        multiMap.put(0, 3);

        Collection<Integer> collection = multiMap.get(0);

        assertTrue(collection instanceof Set);
    }

    @Test
    public void testGet_withSetBackedValueCollection_onEmptyMultiMap() throws Exception {
        MultiMap<Integer, Integer> multiMap = client.getMultiMap(SET_MAP);
        Collection<Integer> collection = multiMap.get(0);

        assertTrue(collection instanceof Set);
    }

    @Test
    public void testGet_withListBackedValueCollection() throws Exception {
        MultiMap<Integer, Integer> multiMap = client.getMultiMap(LIST_MAP);

        multiMap.put(0, 1);
        multiMap.put(0, 2);
        multiMap.put(0, 3);

        Collection<Integer> collection = multiMap.get(0);

        assertTrue(collection instanceof List);
    }

    @Test
    public void testGet_withListBackedValueCollection_onEmptyMultiMap() throws Exception {
        MultiMap<Integer, Integer> multiMap = client.getMultiMap(LIST_MAP);
        Collection<Integer> collection = multiMap.get(0);

        assertTrue(collection instanceof List);
    }


    @Test
    public void testRemove_withSetBackedValueCollection() throws Exception {
        MultiMap<Integer, Integer> multiMap = client.getMultiMap(SET_MAP);

        multiMap.put(0, 1);
        multiMap.put(0, 2);
        multiMap.put(0, 3);

        Collection<Integer> collection = multiMap.remove(0);

        assertTrue(collection instanceof Set);
    }

    @Test
    public void testRemove_withSetBackedValueCollection_onEmptyMultiMap() throws Exception {
        MultiMap<Integer, Integer> multiMap = client.getMultiMap(SET_MAP);
        Collection<Integer> collection = multiMap.remove(0);

        assertTrue(collection instanceof Set);
    }

    @Test
    public void testRemove_withListBackedValueCollection() throws Exception {
        MultiMap<Integer, Integer> multiMap = client.getMultiMap(LIST_MAP);

        multiMap.put(0, 1);
        multiMap.put(0, 2);
        multiMap.put(0, 3);

        Collection<Integer> collection = multiMap.remove(0);

        assertTrue(collection instanceof List);
    }

    @Test
    public void testRemove_withListBackedValueCollection_onEmptyMultiMap() throws Exception {
        MultiMap<Integer, Integer> multiMap = client.getMultiMap(LIST_MAP);
        Collection<Integer> collection = multiMap.remove(0);

        assertTrue(collection instanceof List);
    }
}