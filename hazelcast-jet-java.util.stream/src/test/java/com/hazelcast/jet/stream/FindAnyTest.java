/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.jet.stream;


import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class FindAnyTest extends JetStreamTestSupport {

    @Test
    public void testFindAny_whenSourceMap() {
        IStreamMap<String, Integer> map = getStreamMap(instance);
        fillMap(map);

        Optional<Map.Entry<String, Integer>> first = map.stream().findAny();

        assertTrue(first.isPresent());
        Map.Entry<String, Integer> entry = first.get();

        assertTrue(map.containsKey(entry.getKey()));
        assertEquals(map.get(entry.getKey()), entry.getValue());
    }

    @Test
    public void findAny_whenSourceEmptyMap() {
        IStreamMap<String, Integer> map = getStreamMap(instance);

        Optional<Map.Entry<String, Integer>> first = map.stream().findAny();

        assertFalse(first.isPresent());
    }

    @Test
    public void testFindAny_whenSourceList() {
        IStreamList<Integer> list = getStreamList(instance);
        fillList(list);

        Optional<Integer> first = list.stream().findFirst();

        assertTrue(first.isPresent());
        assertTrue(list.contains(first.get()));
    }

    @Test
    public void findAny_whenSourceEmptyList() {
        IStreamList<Integer> list = getStreamList(instance);

        Optional<Integer> first = list.stream().findAny();

        assertFalse(first.isPresent());
    }
}
