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

package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.*;
import com.hazelcast.test.HazelcastJUnit4ClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;;

@RunWith(HazelcastJUnit4ClassRunner.class)
@Category(ParallelTest.class)
public class MapReduceTest extends HazelcastTestSupport {
    @Test
    public void testMapCombineReduceWithObject() throws InterruptedException {
        doTest(true, MapConfig.InMemoryFormat.OBJECT);
    }

    @Test
    public void testMapReduceWithObject() throws InterruptedException {
        doTest(false, MapConfig.InMemoryFormat.OBJECT);
    }

    @Test
    public void testMapCombineReduceWithCached() throws InterruptedException {
        doTest(true, MapConfig.InMemoryFormat.CACHED);
    }

    @Test
    public void testMapReduceWithCached() throws InterruptedException {
        doTest(false, MapConfig.InMemoryFormat.CACHED);
    }

    @Test
    public void testMapCombineReduceWithBinary() throws InterruptedException {
        doTest(true, MapConfig.InMemoryFormat.BINARY);
    }

    @Test
    public void testMapReduceWithBinary() throws InterruptedException {
        doTest(false, MapConfig.InMemoryFormat.BINARY);
    }

    private void doTest(boolean useCombiner, MapConfig.InMemoryFormat format) {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = new Config();
        cfg.getMapConfig("default").setInMemoryFormat(format);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);
        IMap<Integer, String> map = instance1.getMap("testMapEntryProcessor");
        Map<Character,Long> expectedOutput = new HashMap<Character,Long>();

        int size = 10000;
        for (int i = 0; i < size; i++) {
            String value = "this is a test: " + i;
            map.put(i, value);

            for (int ei = 0, ee = value.length(); ei < ee; ++ei) {
                Character key = value.charAt(ei);
                Long evalue = expectedOutput.get(key);
                if (evalue == null) {
                    evalue = 0L;
                }

                evalue = evalue + 1L;
                expectedOutput.put(key, evalue);
            }
        }

        EntryMapper<Integer,String,Character,Long> mapper = new TestMapper();
        EntryReducer<Character,Long,Character,Long> combiner = new TestCombiner();
        EntryReducer<Character,Long,Character,Long> reducer = new TestReducer();

        Map<Character,Long> result = map.mapReduce(mapper, useCombiner ? combiner : null, reducer);

        for (Map.Entry<Character,Long> entry : result.entrySet()) {
            System.out.println("result: '" + entry.getKey() + "' = " + entry.getValue());

            Long expected = expectedOutput.get(entry.getKey());
            assertNotNull(expected);
            assertEquals(expected, entry.getValue());
        }

        assertEquals(expectedOutput.keySet(), result.keySet());
        instance1.getLifecycleService().shutdown();
        instance2.getLifecycleService().shutdown();
    }

    private static class TestMapper implements EntryMapper<Integer,String,Character,Long>, Serializable {
        private static final long serialVersionUID = 1610394521391949724L;

        @Override
        public void process(Integer key, String value, MapReduceOutput<Character,Long> output) {
            for (int i = 0, e = value.length(); i < e; ++i) {
                output.write(value.charAt(i), 1L);
            }
        }
    }

    private static class TestCombiner implements EntryReducer<Character,Long,Character,Long>, Serializable {
        private static final long serialVersionUID = -7276573659756130691L;

        @Override
        public void process(Character key, Iterable<Long> values, MapReduceOutput<Character,Long> output) {
            long num = 0L;
            for (Long value : values) {
                num += value.longValue();
            }

            output.write(key, num);
        }
    }

    private static class TestReducer implements EntryReducer<Character,Long,Character,Long>, Serializable {
        private static final long serialVersionUID = -5252182752495503851L;

        @Override
        public void process(Character key, Iterable<Long> values, MapReduceOutput<Character,Long> output) {
            long num = 0L;
            for (Long value : values) {
                num += value.longValue();
            }

            output.write(key, num);
        }
    }
}
