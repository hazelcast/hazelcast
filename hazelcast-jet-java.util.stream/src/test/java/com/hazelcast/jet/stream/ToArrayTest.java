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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class ToArrayTest extends JetStreamTestSupport {

    @Test
    public void testToArray_whenSourceMap() {
        IStreamMap<String, Integer> map = getStreamMap(instance);
        fillMap(map);

        Object[] objects = map.stream().toArray();

        assertEquals(COUNT, objects.length);
        for (int i = 0; i < COUNT; i++) {
            assertInstanceOf(Map.Entry.class, objects[i]);
            assertNotNull(objects[i]);
        }
    }

    @Test
    public void testToArrayWithGenerator_whenSourceMap() {
        IStreamMap<String, Integer> map = getStreamMap(instance);
        fillMap(map);

        Map.Entry[] entries = map.stream().toArray(Map.Entry[]::new);

        assertEquals(COUNT, entries.length);
        for (int i = 0; i < COUNT; i++) {
            assertNotNull(entries[i]);
        }
    }

    @Test
    public void testToArray_whenSourceList() {
        IStreamList<Integer> list = getStreamList(instance);
        fillList(list);

        Object[] objects = list.stream().toArray();

        assertEquals(COUNT, objects.length);
        for (int i = 0; i < COUNT; i++) {
            assertEquals(i, objects[i]);
        }
    }

    @Test
    public void testToArrayWithGenerator_whenSourceList() {
        IStreamList<Integer> list = getStreamList(instance);
        fillList(list);

        Integer[] elements = list.stream().toArray(Integer[]::new);

        assertEquals(COUNT, elements.length);
        for (int i = 0; i < COUNT; i++) {
            assertEquals(i, (int)elements[i]);
        }
    }

}
