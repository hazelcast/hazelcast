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

import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.core.test.TestSupport;
import com.hazelcast.jet.function.SupplierEx;
import com.hazelcast.jet.impl.JetEvent;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.core.JetTestSupport.wm;
import static com.hazelcast.jet.impl.JetEvent.jetEvent;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

@Category(ParallelTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class TransformStatefulPTest {

    @Test
    public void mapStateful_noTTL() {
        SupplierEx<Processor> supplier = Processors.mapStatefulP(
                0,
                Entry::getKey,
                e -> 0L,
                () -> new long[1],
                (long[] s, Entry<String, Long> e) -> {
                    s[0] += e.getValue();
                    return s[0];
                },
                (e, k, r) -> entry(k, r));

        TestSupport.verifyProcessor(supplier)
                   .input(asList(
                           entry("a", 1L),
                           entry("b", 2L),
                           entry("a", 3L),
                           entry("b", 4L)
                   ))
                   .expectOutput(asList(
                           entry("a", 1L),
                           entry("b", 2L),
                           entry("a", 4L),
                           entry("b", 6L)
                   ));
    }

    @Test
    public void mapStateful_toNull_inMapToOutputFn() {
        SupplierEx<Processor> supplier = Processors.mapStatefulP(
                0,
                Entry::getKey,
                e -> 0L,
                () -> new long[1],
                (long[] s, Entry<String, Long> e) -> {
                    s[0] += e.getValue();
                    return s[0];
                },
                (e, k, r) -> null);

        TestSupport.verifyProcessor(supplier)
                   .input(singletonList(entry("a", 1L)))
                   .expectOutput(emptyList());
    }

    @Test
    public void mapStateful_toNull_inMapFn() {
        SupplierEx<Processor> supplier = Processors.mapStatefulP(
                0,
                Entry::getKey,
                e -> 0L,
                () -> new long[1],
                (long[] s, Entry<String, Long> e) -> null,
                (e, k, r) -> entry(k, r));

        TestSupport.verifyProcessor(supplier)
                   .input(singletonList(entry("a", 1L)))
                   .expectOutput(emptyList());
    }

    @Test
    public void mapStateful_withTTL() {
        SupplierEx<Processor> supplier = Processors.mapStatefulP(
                2,
                jetEvent -> jetEvent.payload().getKey(),
                JetEvent::timestamp,
                () -> new long[1],
                (long[] s, JetEvent<Entry<String, Long>> e) -> {
                    s[0] += e.payload().getValue();
                    return s[0];
                },
                (event, k, r) -> jetEvent(event.timestamp(), entry(k, r))
        );

        TestSupport.verifyProcessor(supplier)
                   .input(asList(
                           jetEvent(0, entry("a", 1L)),
                           jetEvent(1, entry("b", 2L)),
                           wm(3), // evict a
                           jetEvent(3, entry("a", 3L)),
                           wm(4), // evict b
                           jetEvent(4, entry("b", 4L))
                   ))
                   .expectOutput(asList(
                           jetEvent(0, entry("a", 1L)),
                           jetEvent(1, entry("b", 2L)),
                           wm(3),
                           jetEvent(3, entry("a", 3L)),
                           wm(4),
                           jetEvent(4, entry("b", 4L))
                   ));
    }

    @Test
    public void mapStateful_withTTL_manyKeys() {
        /*
        This test is designed to test TTL handling if not all items are evicted in tryProcessWatermark
        due to the MAX_ITEMS_TO_EVICT.
         */
        SupplierEx<Processor> supplier = Processors.mapStatefulP(
                2,
                jetEvent -> jetEvent.payload().getKey(),
                JetEvent::timestamp,
                () -> new long[1],
                (long[] s, JetEvent<Entry<String, Long>> e) -> {
                    s[0] += e.payload().getValue();
                    return s[0];
                },
                (event, k, r) -> jetEvent(event.timestamp(), entry(k, r))
        );

        // use more keys than MAX_ITEMS_TO_EVICT
        int numKeys = TransformStatefulP.MAX_ITEMS_TO_EVICT + 1;

        // Build the input. First add entries with keys 0..max, then with keys max..0.
        // The reason is that the eviction goes in the order items were processed so
        // after the eviction the keys at the end will remain. And we should evict those
        // instead of continuing to use them.
        List<Object> input = new ArrayList<>();
        for (int i = 0; i < numKeys; i++) {
            input.add(jetEvent(0, entry("k" + i, 1L)));
        }
        input.add(wm(3));
        for (int i = numKeys; i > 0; ) {
            i--;
            input.add(jetEvent(3, entry("k" + i, 3L)));
        }

        TestSupport.verifyProcessor(supplier)
                   .input(input)
                   .disableLogging()
                   .expectOutput(input);
    }

    @Test
    public void mapStateful_lateEvent() {
        SupplierEx<Processor> supplier = Processors.mapStatefulP(
                0,
                jetEvent -> 0L,
                JetEvent::timestamp,
                () -> new long[1],
                (long[] s, JetEvent<Long> e) -> {
                    s[0] += e.payload();
                    return s[0];
                },
                (event, k, r) -> jetEvent(event.timestamp(), r)
        );

        TestSupport.verifyProcessor(supplier)
                   .input(asList(
                           jetEvent(0, 1L),
                           jetEvent(1, 2L),
                           wm(3), // evict a
                           jetEvent(0, 1L)
                   ))
                   .expectOutput(asList(
                           jetEvent(0, 1L),
                           jetEvent(1, 3L),
                           wm(3)
        ));
    }

    @Test
    public void mapStateful_negativeWmTime() {
        SupplierEx<Processor> supplier = Processors.mapStatefulP(
                2,
                jetEvent -> jetEvent.payload().getKey(),
                JetEvent::timestamp,
                () -> new long[1],
                (long[] s, JetEvent<Entry<String, Long>> e) -> {
                    s[0] += e.payload().getValue();
                    return s[0];
                },
                (event, k, r) -> jetEvent(event.timestamp(), entry(k, r))
        );

        TestSupport.verifyProcessor(supplier)
                   .input(asList(
                           jetEvent(-10, entry("a", 1L)),
                           jetEvent(-9, entry("b", 2L)),
                           wm(-7), // evict a
                           jetEvent(-7, entry("a", 3L)),
                           jetEvent(-7, entry("b", 3L)),
                           wm(-4), // evict b
                           jetEvent(-4, entry("b", 4L))
                   ))
                   .expectOutput(asList(
                           jetEvent(-10, entry("a", 1L)),
                           jetEvent(-9, entry("b", 2L)),
                           wm(-7),
                           jetEvent(-7, entry("a", 3L)),
                           jetEvent(-7, entry("b", 5L)),
                           wm(-4),
                           jetEvent(-4, entry("b", 4L))
                   ));
    }

    @Test
    public void flatMapStateful() {
        SupplierEx<Processor> supplier = Processors.flatMapStatefulP(
                0,
                Entry::getKey,
                e -> 0L,
                () -> new long[1],
                (long[] s, Entry<String, Long> e) -> {
                    s[0] += e.getValue();
                    return Traversers.traverseItems(s[0], -s[0]);
                },
                (e, k, r) -> entry(k, r));

        TestSupport.verifyProcessor(supplier)
                   .input(asList(
                           entry("a", 1L),
                           entry("b", 2L),
                           entry("a", 3L),
                           entry("b", 4L)
                   ))
                   .expectOutput(asList(
                           entry("a", 1L),
                           entry("a", -1L),
                           entry("b", 2L),
                           entry("b", -2L),
                           entry("a", 4L),
                           entry("a", -4L),
                           entry("b", 6L),
                           entry("b", -6L)
                   ));
    }
}
