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

import org.junit.Test;

import java.util.Iterator;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class IteratorTest extends AbstractStreamTest {

    @Test
    public void testIterator_whenSourceMap() {
        IStreamMap<String, Integer> map = getMap();
        fillMap(map);

        Iterator<Map.Entry<String, Integer>> iterator = map.stream().iterator();

        int count = 0;
        while (iterator.hasNext()) {
            Map.Entry<String, Integer> next = iterator.next();
            assertNotNull(next);
            count++;
        }
        assertEquals(COUNT, count);
    }

    @Test
    public void testIterator_whenSourceList() {
        IStreamList<Integer> list = getList();
        fillList(list);

        Iterator<Integer> iterator = list.stream().iterator();

        int count = 0;
        while (iterator.hasNext()) {
            assertEquals(count, (int) iterator.next());
            count++;
        }
        assertEquals(COUNT, count);
    }

}
