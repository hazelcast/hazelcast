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

package com.hazelcast.jet.impl.processor;

import com.hazelcast.jet.core.test.TestSupport;
import com.hazelcast.jet.datamodel.TimestampedEntry;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.HashSet;
import java.util.Set;

import static com.hazelcast.jet.aggregate.AggregateOperations.toSet;
import static com.hazelcast.jet.core.SlidingWindowPolicy.slidingWinPolicy;
import static com.hazelcast.jet.core.processor.Processors.combineToSlidingWindowP;
import static java.util.Arrays.asList;

@Category(ParallelTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class SlidingWindowP_FrameCombiningTest {

    private static final Long KEY = 77L;

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void when_multipleFrames_then_combine() {
        TestSupport
                .verifyProcessor(
                        combineToSlidingWindowP(slidingWinPolicy(8, 4), toSet(), TimestampedEntry::fromWindowResult))
                .input(asList(
                        frame(2, set("a")),
                        frame(4, set("b")),
                        frame(6, set("c"))
                ))
                .expectOutput(asList(
                        frame(4, set("a", "b")),
                        frame(8, set("a", "b", "c")),
                        frame(12, set("c"))
                ));
    }

    private <V> TimestampedEntry<Long, V> frame(long ts, V value) {
        return new TimestampedEntry<>(ts, KEY, value);
    }

    @SafeVarargs
    private final <E> Set<E> set(E ... elements) {
        return new HashSet(asList(elements));
    }
}
