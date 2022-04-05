/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.processor;

import com.hazelcast.function.Functions;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.core.TimestampKind;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.core.test.TestSupport;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.Map.Entry;

import static com.hazelcast.function.Functions.entryKey;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.aggregate.AggregateOperations.aggregateOperation2;
import static com.hazelcast.jet.core.SlidingWindowPolicy.tumblingWinPolicy;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

@Category(ParallelJVMTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class SlidingWindowP_CoGroupTest {

    @Test
    @SuppressWarnings("unchecked")
    public void test() {
        SupplierEx supplier = Processors.aggregateToSlidingWindowP(
                asList(Functions.<String>entryKey(), entryKey()),
                asList(t -> 1L, t -> 1L),
                TimestampKind.FRAME,
                tumblingWinPolicy(1),
                0L,
                aggregateOperation2(
                        AggregateOperations.<Entry<String, String>>toList(),
                        AggregateOperations.<Entry<String, String>>toList()),
                (start, end, key, result, isEarly) -> result(end, key, result.f0(), result.f1()));
        Entry<String, String> entry1 = entry("k1", "a");
        Entry<String, String> entry2 = entry("k2", "b");
        Entry<String, String> entry3 = entry("k1", "c");
        Entry<String, String> entry4 = entry("k3", "d");
        Entry<String, String> entry5 = entry("k1", "e");
        TestSupport.verifyProcessor(supplier)
                   .inputs(asList(
                           asList(entry1, entry2),
                           asList(entry3, entry4, entry5)
                   ))
                   .expectOutput(asList(
                           result(1, "k1", singletonList(entry1), asList(entry3, entry5)),
                           result(1, "k2", singletonList(entry2), emptyList()),
                           result(1, "k3", emptyList(), singletonList(entry4))
                   ));
    }

    private static String result(
            long winEnd,
            String key,
            List<Entry<String, String>> result1,
            List<Entry<String, String>> result2
    ) {
        return String.format("(%03d, %s: %s, %s)", winEnd, key, result1, result2);
    }
}
