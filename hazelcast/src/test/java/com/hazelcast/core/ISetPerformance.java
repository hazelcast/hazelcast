/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

import java.util.Arrays;

import static org.junit.Assert.assertTrue;

@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class ISetPerformance extends PerformanceTest {
    private ISet<String> set = Hazelcast.getSet("ISetPerformance");

    @After
    public void clear() {
        t.stop();
        t.printResult();
        set.clear();
        assertTrue(set.isEmpty());
    }

    @Test
    public void testSetAddSameValue() {
        String test = "testSetAddSameValue";
        t = new PerformanceTimer(test, ops);
        for (int i = 0; i < ops; ++i) {
            set.add("Hello World");
        }
    }

    @Test
    public void testSetAddDifferentValue() {
        String test = "testSetAddDifferentValue";
        t = new PerformanceTimer(test, ops);
        for (int i = 0; i < ops; ++i) {
            set.add("Hello" + i);
        }
    }

    @Test
    public void testSetAddAllSameValues() {
        String test = "testSetAddAllSameValues";
        String[] values = {"one", "two", "three", "four", "five"};
        t = new PerformanceTimer(test, ops);
        for (int i = 0; i < ops; ++i) {
            set.addAll(Arrays.asList(values));
        }
    }

    @Test
    public void testSetAddAllDifferentValues() {
        String test = "testListAddAllDifferentValues";
        t = new PerformanceTimer(test, ops);
        for (int i = 0; i < ops; ++i) {
            String[] values = {"one" + i, "two" + i, "three" + i, "four" + i, "five" + i};
            set.addAll(Arrays.asList(values));
        }
    }

    @Test
    public void testSetContains() {
        String test = "testSetContains";
        for (int i = 0; i < ops; ++i) {
            set.add("Hello" + i);
        }
        t = new PerformanceTimer(test, ops);
        for (int i = 0; i < ops; ++i) {
            set.contains("Hello" + i);
        }
    }

    @Test
    public void testSetContainsAll() {
        String test = "testSetContainsAll";
        for (int i = 0; i < ops; ++i) {
            String[] values = {"one" + i, "two" + i, "three" + i, "four" + i, "five" + i};
            set.addAll(Arrays.asList(values));
        }
        t = new PerformanceTimer(test, ops);
        for (int i = 0; i < ops; ++i) {
            String[] values = {"one" + i, "two" + i, "three" + i, "four" + i, "five" + i};
            set.containsAll(Arrays.asList(values));
        }
    }

    @Test
    public void testSetRemove() {
        String test = "testSetRemove";
        for (int i = 0; i < ops; ++i) {
            set.add("Hello" + i);
        }
        t = new PerformanceTimer(test, ops);
        for (int i = 0; i < ops; ++i) {
            set.remove("Hello" + i);
        }
    }

    @Test
    public void testSetRemoveAll() {
        String test = "testSetRemoveAll";
        ops /= 10;
        for (int i = 0; i < ops; ++i) {
            String[] values = {"one" + i, "two" + i, "three" + i, "four" + i, "five" + i};
            set.addAll(Arrays.asList(values));
        }
        t = new PerformanceTimer(test, ops);
        for (int i = 0; i < ops; ++i) {
            String[] values = {"one" + i, "two" + i, "three" + i, "four" + i, "five" + i};
            set.removeAll(Arrays.asList(values));
        }
        ops *= 10;
    }

    @Test
    public void testSetRetainAll() {
        String test = "testSetRetainAll";
        for (int i = 0; i < ops; ++i) {
            set.add("Hello" + i);
        }
        String[] values = {"Hello1", "Hello2", "Hello3", "Hello4", "Hello5"};
        t = new PerformanceTimer(test, ops);
        set.retainAll(Arrays.asList(values));
    }
}
