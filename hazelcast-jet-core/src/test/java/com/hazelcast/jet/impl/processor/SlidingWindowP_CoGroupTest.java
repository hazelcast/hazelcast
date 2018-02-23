/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.core.TimestampKind;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.core.test.TestSupport;
import com.hazelcast.jet.datamodel.TimestampedEntry;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map.Entry;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.core.SlidingWindowPolicy.tumblingWinPolicy;
import static com.hazelcast.jet.datamodel.TwoBags.twoBags;
import static com.hazelcast.jet.function.DistributedFunctions.entryKey;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

@Category(ParallelTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class SlidingWindowP_CoGroupTest {

    @Test
    public void test() {
        DistributedSupplier supplier = Processors.aggregateToSlidingWindowP(
                asList(entryKey(), entryKey()),
                asList(t -> 1L, t -> 1L),
                TimestampKind.FRAME,
                tumblingWinPolicy(1),
                AggregateOperations.toTwoBags(),
                TimestampedEntry::new);

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
                           new TimestampedEntry<>(1, "k1", twoBags(singletonList(entry1), asList(entry3, entry5))),
                           new TimestampedEntry<>(1, "k2", twoBags(singletonList(entry2), emptyList())),
                           new TimestampedEntry<>(1, "k3", twoBags(emptyList(), singletonList(entry4)))
                   ));
    }
}
