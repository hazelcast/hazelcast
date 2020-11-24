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

package com.hazelcast.map.impl.query;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.SampleTestObjects;
import com.hazelcast.query.impl.predicates.InstanceOfPredicate;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static com.hazelcast.test.Accessors.getSerializationService;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapValuesTest extends HazelcastTestSupport {

    private HazelcastInstance instance;
    private IMap<String, String> map;
    private SerializationService serializationService;

    @Before
    public void setup() {
        instance = createHazelcastInstance();
        map = instance.getMap(randomName());
        serializationService = getSerializationService(instance);
    }

    @Test(expected = NullPointerException.class)
    public void whenPredicateNull() {
        map.values(null);
    }

    @Test
    public void whenMapEmpty() {
        Collection<String> result = map.values(Predicates.alwaysTrue());

        assertTrue(result.isEmpty());
    }

    @Test
    public void whenSelecting_withoutPredicate() {
        map.put("1", "a");
        map.put("2", "b");
        map.put("3", "c");

        Collection<String> result = map.values();

        assertEquals(3, result.size());
        assertContains(result, "a");
        assertContains(result, "b");
        assertContains(result, "c");
    }

    @Test
    public void whenSelectingAllEntries() {
        map.put("1", "a");
        map.put("2", "b");
        map.put("3", "c");

        Collection<String> result = map.values(Predicates.alwaysTrue());

        assertEquals(3, result.size());
        assertContains(result, "a");
        assertContains(result, "b");
        assertContains(result, "c");
    }

    @Test
    public void whenSelectingSomeEntries() {
        map.put("1", "good1");
        map.put("2", "bad");
        map.put("3", "good2");

        Collection<String> result = map.values(new GoodPredicate());

        assertEquals(2, result.size());
        assertContains(result, "good1");
        assertContains(result, "good2");
    }

    @Test
    public void whenSelectingPartitionSubset() {
        NodeEngineImpl nodeEngine = getNodeEngineImpl(instance);
        int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        PartitionIdSet partitionSubset =
                new PartitionIdSet(partitionCount, IntStream.range(0, partitionCount / 2).boxed().collect(Collectors.toList()));
        Set<String> matchingKeys = new HashSet<>();
        for (int i = 0; i < 10; i++) {
            String key = String.valueOf(i);
            map.put(key, key);
            if (partitionSubset.contains(nodeEngine.getPartitionService().getPartitionId(key))) {
                matchingKeys.add(key);
            }
        }
        // assert test sanity
        assertBetween("keyCount", matchingKeys.size(), 1, map.size() - 1);

        Collection<String> result = ((MapProxyImpl<String, String>) map).values(Predicates.alwaysTrue(), partitionSubset);
        assertEquals(matchingKeys, result);
    }

    @Test
    public void testResultType() {
        map.put("1", "a");
        Collection<String> entries = map.values(Predicates.alwaysTrue());

        QueryResultCollection collection = assertInstanceOf(QueryResultCollection.class, entries);
        QueryResultRow row = (QueryResultRow) collection.getRows().iterator().next();
        // there should only be a value, no key
        assertNull(row.getKey());
        assertEquals(serializationService.toData("a"), row.getValue());
    }

    @Test
    public void testSerializationServiceNullClassLoaderProblem() {
        // if the classloader is null the following call throws NullPointerException
        map.values(new InstanceOfPredicate(SampleTestObjects.PortableEmployee.class));
    }

    static class GoodPredicate implements Predicate<String, String> {
        @Override
        public boolean apply(Map.Entry<String, String> mapEntry) {
            return mapEntry.getValue().startsWith("good");
        }
    }
}
