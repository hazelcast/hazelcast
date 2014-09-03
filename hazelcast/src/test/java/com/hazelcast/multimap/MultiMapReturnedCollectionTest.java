/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.multimap;

import com.hazelcast.config.Config;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MultiMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class MultiMapReturnedCollectionTest extends HazelcastTestSupport {

    @Test
    public void testGet_returnsCorrectCollectionType_withSet() throws Exception {
        MultiMap<Integer, Integer> multiMap = createMultiMapWithCollectionType(MultiMapConfig.ValueCollectionType.SET, 2);

        multiMap.put(0, 1);
        multiMap.put(0, 2);
        multiMap.put(0, 3);

        Collection<Integer> collection = multiMap.get(0);

        assertTrue(collection instanceof Set);
    }

    @Test
    public void testGet_returnsCorrectCollectionType_whenMultiMapIsEmpty_withSet() throws Exception {
        MultiMap<Integer, Integer> multiMap = createMultiMapWithCollectionType(MultiMapConfig.ValueCollectionType.SET, 2);
        Collection<Integer> collection = multiMap.get(0);

        assertTrue(collection instanceof Set);
    }

    @Test
    public void testGet_returnsCorrectCollectionType_withList() throws Exception {
        MultiMap<Integer, Integer> multiMap = createMultiMapWithCollectionType(MultiMapConfig.ValueCollectionType.LIST, 2);

        multiMap.put(0, 1);
        multiMap.put(0, 2);
        multiMap.put(0, 3);

        Collection<Integer> collection = multiMap.get(0);

        assertTrue(collection instanceof List);
    }

    @Test
    public void testGet_returnsCorrectCollectionType_whenMultiMapIsEmpty_withList() throws Exception {
        MultiMap<Integer, Integer> multiMap = createMultiMapWithCollectionType(MultiMapConfig.ValueCollectionType.LIST, 2);
        Collection<Integer> collection = multiMap.get(0);

        assertTrue(collection instanceof List);
    }

    @Test
    public void testRemove_returnsCorrectCollectionType_withSet() throws Exception {
        MultiMap<Integer, Integer> multiMap = createMultiMapWithCollectionType(MultiMapConfig.ValueCollectionType.SET, 2);

        multiMap.put(0, 1);
        multiMap.put(0, 2);
        multiMap.put(0, 3);

        Collection<Integer> collection = multiMap.remove(0);

        assertTrue(collection instanceof Set);
    }

    @Test
    public void testRemove_returnsCorrectCollectionType_whenMultiMapIsEmpty_withSet() throws Exception {
        MultiMap<Integer, Integer> multiMap = createMultiMapWithCollectionType(MultiMapConfig.ValueCollectionType.SET, 2);
        Collection<Integer> collection = multiMap.remove(0);

        assertTrue(collection instanceof Set);
    }

    @Test
    public void testRemove_returnsCorrectCollectionType_withList() throws Exception {
        MultiMap<Integer, Integer> multiMap = createMultiMapWithCollectionType(MultiMapConfig.ValueCollectionType.LIST, 2);

        multiMap.put(0, 1);
        multiMap.put(0, 2);
        multiMap.put(0, 3);

        Collection<Integer> collection = multiMap.remove(0);

        assertTrue(collection instanceof List);
    }

    @Test
    public void testRemove_returnsCorrectCollectionType_whenMultiMapIsEmpty_withList() throws Exception {
        MultiMap<Integer, Integer> multiMap = createMultiMapWithCollectionType(MultiMapConfig.ValueCollectionType.LIST, 2);
        Collection<Integer> collection = multiMap.remove(0);

        assertTrue(collection instanceof List);
    }




    private MultiMap<Integer, Integer> createMultiMapWithCollectionType(MultiMapConfig.ValueCollectionType collectionType, int nodeCount) {
        String multiMapName = randomMapName();
        Config config = new Config();
        config.getMultiMapConfig(multiMapName).setValueCollectionType(collectionType);
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(nodeCount);
        final HazelcastInstance[] instances = factory.newInstances(config);
        return instances[0].getMultiMap(multiMapName);
    }
}
