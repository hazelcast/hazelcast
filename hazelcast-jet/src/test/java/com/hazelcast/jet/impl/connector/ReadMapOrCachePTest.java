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

package com.hazelcast.jet.impl.connector;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.core.processor.SourceProcessors;
import com.hazelcast.jet.core.test.TestSupport;
import com.hazelcast.map.IMap;
import com.hazelcast.projection.Projection;
import com.hazelcast.query.Predicate;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import static com.hazelcast.jet.TestContextSupport.adaptSupplier;
import static com.hazelcast.jet.Util.entry;
import static java.util.Collections.emptyList;

@RunWith(HazelcastSerialClassRunner.class)
public class ReadMapOrCachePTest extends SimpleTestInClusterSupport {

    @BeforeClass
    public static void setUp() {
        initialize(1, null);
    }

    @Test
    public void test_whenEmpty() {
        TestSupport
                .verifyProcessor(adaptSupplier(SourceProcessors.readMapP(randomMapName())))
                .jetInstance(instance())
                .disableSnapshots()
                .disableProgressAssertion()
                .expectOutput(emptyList());
    }

    @Test
    public void test_whenNoPredicateAndNoProjection() {
        IMap<Integer, String> map = instance().getMap(randomMapName());
        List<Entry<Integer, String>> expected = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            map.put(i, "value-" + i);
            expected.add(entry(i, "value-" + i));
        }

        TestSupport
                .verifyProcessor(adaptSupplier(SourceProcessors.readMapP(map.getName())))
                .jetInstance(instance())
                .disableSnapshots()
                .disableProgressAssertion()
                .outputChecker(TestSupport.SAME_ITEMS_ANY_ORDER)
                .expectOutput(expected);
    }

    @Test
    public void test_whenPredicateAndProjectionSet() {
        IMap<Integer, String> map = instance().getMap(randomMapName());
        List<String> expected = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            map.put(i, "value-" + i);
            if (i % 2 == 0) {
                expected.add("value-" + i);
            }
        }

        Predicate<Integer, String> predicate = entry -> entry.getKey() % 2 == 0;
        Projection<Entry<Integer, String>, String> projection = toProjection(Entry::getValue);
        TestSupport
                .verifyProcessor(adaptSupplier(SourceProcessors.readMapP(map.getName(), predicate, projection)))
                .jetInstance(instance())
                .disableSnapshots()
                .disableProgressAssertion()
                .outputChecker(TestSupport.SAME_ITEMS_ANY_ORDER)
                .expectOutput(expected);
    }

    @Test
    public void test_whenProjectedToObjectWithNoEquals() {
        // test for https://github.com/hazelcast/hazelcast-jet/issues/2448
        IMap<Integer, Object[]> map = instance().getMap(randomMapName());
        // two values are enough: TestSupport always uses outbox limited to 1 item to drive the processors crazy
        map.put(0, new Object[0]);
        map.put(1, new Object[0]);

        TestSupport
                .verifyProcessor(adaptSupplier(SourceProcessors.readMapP(map.getName())))
                .jetInstance(instance())
                .disableSnapshots()
                .disableProgressAssertion()
                .outputChecker((expected, actual) -> 2 == actual.size())
                .expectOutput(emptyList());
    }

    private static <I, O> Projection<I, O> toProjection(FunctionEx<I, O> projectionFn) {
        return (Projection<I, O>) projectionFn::apply;
    }
}
