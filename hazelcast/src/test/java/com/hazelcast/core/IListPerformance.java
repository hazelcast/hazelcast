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

package com.hazelcast.core;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;

import static junit.framework.Assert.assertTrue;

@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class IListPerformance extends PerformanceTest {
    private IList<Integer> list = Hazelcast.getList("IListPerformance");

    @After
    public void clear() {
        t.stop();
        t.printResult();
        list.clear();
        assertTrue(list.isEmpty());
    }

    @Test
    public void testListAdd() {
        String test = "testListAdd";
        t = new PerformanceTimer(test, ops);
        for (int i = 0; i < ops; ++i) {
            list.add(i);
        }
    }

    @Test
    public void testListContains() {
        String test = "testListContains";
        for (int i = 0; i < ops; ++i) {
            list.add(i);
        }
        t = new PerformanceTimer(test, ops);
        for (int i = 0; i < ops; ++i) {
            list.contains(i);
        }
    }

    @Test
    public void testListIterator() {
        String test = "testListIterator";
        for (int i = 0; i < ops; ++i) {
            list.add(i);
        }
        t = new PerformanceTimer(test, ops);
        for (Integer aList : list) {
            int a = aList;
        }
    }
}
