/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class PipelineTest extends HazelcastTestSupport {

    private HazelcastInstance hz;

    @Before
    public void before() {
        hz = createHazelcastInstance();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructor_whenNegativeDepth() {
        new Pipeline<String>(0);
    }

    @Test(expected = NullPointerException.class)
    public void add_whenNull() throws InterruptedException {
        Pipeline<String> pipeline = new Pipeline<String>(1);
        pipeline.add(null);
    }

    @Test
    public void test() throws Exception {
        IMap map = hz.getMap("map");
        int items = 100000;
        List<Integer> expected = new ArrayList<Integer>();
        Random random = new Random();
        for (int k = 0; k < items; k++) {
            int item = random.nextInt();
            expected.add(item);
            map.put(k, item);
        }

        Pipeline<String> pipeline = new Pipeline<String>(1);
        for (int k = 0; k < items; k++) {
            pipeline.add(map.getAsync(k));
        }

        assertEquals(expected, pipeline.results());
    }
}
