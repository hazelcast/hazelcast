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

import com.hazelcast.core.IList;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
@Ignore
public class Demo extends JetStreamTestSupport {

    @Test
    public void testFilterSquares() {
        IStreamMap<String, Integer> map = getStreamMap(instance);
        fillMap(map);

        IList<Integer> list = map.stream()
                .map(Map.Entry::getValue)
                .filter(e -> (int) Math.sqrt(e) == Math.sqrt(e))
                .sorted()
                .collect(DistributedCollectors.toIList());

        for (Integer integer : list) {
            System.out.println(integer);
        }
    }

    @Test
    public void testSumAllValues() {
        IStreamMap<String, Integer> map = getStreamMap(instance);
        fillMap(map);

        Integer sum = map.stream()
                .map(Map.Entry::getValue)
                .reduce(0, (left, right) -> left + right);

        System.out.println("Sum=" + sum);
        System.out.println("Expected=" + COUNT*(COUNT-1)/2);
    }

    @Test
    public void testDistinctValues() {
        IStreamMap<String, Integer> map = getStreamMap(instance);
        fillMap(map);

        IList<Integer> distinct = map.stream()
                .map(Map.Entry::getValue)
                .map(m -> m % 10)
                .distinct()
                .collect(DistributedCollectors.toIList());

        for (Integer integer : distinct) {
            System.out.println(integer);
        }
    }
}
