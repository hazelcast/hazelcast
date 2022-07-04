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

import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.function.ToLongFunctionEx;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.core.test.TestSupport;
import com.hazelcast.jet.function.TriFunction;
import com.hazelcast.jet.impl.JetEvent;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.hazelcast.jet.Traversers.traverseItems;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.core.JetTestSupport.wm;
import static com.hazelcast.jet.impl.JetEvent.jetEvent;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

@Category({QuickTest.class, ParallelJVMTest.class})
@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@SuppressWarnings("checkstyle:declarationorder")
public class TransformStatefulPTest {

    private final Function<Entry<Object, Long>, Traverser<Entry<Object, Long>>> expandEntryFn =
            en -> traverseItems(en, entry(en.getKey(), -en.getValue()));
    private final Function<JetEvent<Entry<Object, Long>>, Traverser<JetEvent<Entry<Object, Long>>>> expandJetEventFn =
            je -> traverseItems(je, jetEvent(je.timestamp(), entry(je.payload().getKey(), -je.payload().getValue())));

    @Parameter
    public boolean flatMap;

    @Parameters(name = "flatMap={0}")
    public static Collection<Object> parameters() {
        return asList(true, false);
    }

    @Test
    public void mapStateful_noTtl() {
        SupplierEx<Processor> supplier = createSupplier(
                0,
                Entry::getKey,
                e -> 0L,
                () -> new long[1],
                (long[] s, Object k, Entry<String, Long> e) -> {
                    s[0] += e.getValue();
                    return entry(k, s[0]);
                },
                null,
                expandEntryFn);

        TestSupport.verifyProcessor(supplier)
                   .input(asList(
                           entry("a", 1L),
                           entry("b", 2L),
                           entry("a", 3L),
                           entry("b", 4L)
                   ))
                   .expectOutput(asExpandedList(expandEntryFn,
                           entry("a", 1L),
                           entry("b", 2L),
                           entry("a", 4L),
                           entry("b", 6L)
                   ));
    }

    @Test
    public void mapStateful_toNull_inMapFn() {
        SupplierEx<Processor> supplier = createSupplier(
                0,
                Entry::getKey,
                e -> 0L,
                () -> new long[1],
                (long[] s, Object k, Entry<String, Long> e) -> null,
                null,
                expandEntryFn);

        TestSupport.verifyProcessor(supplier)
                   .input(singletonList(entry("a", 1L)))
                   .expectOutput(emptyList());
    }

    @Test
    public void mapStateful_withTtl() {
        SupplierEx<Processor> supplier = createSupplier(
                2,
                jetEvent -> jetEvent.payload().getKey(),
                JetEvent::timestamp,
                () -> new long[1],
                (long[] s, Object k, JetEvent<Entry<String, Long>> e) -> {
                    s[0] += e.payload().getValue();
                    return jetEvent(e.timestamp(), entry(k, s[0]));
                },
                null,
                expandJetEventFn
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
                   .expectOutput(asExpandedList(expandJetEventFn,
                           jetEvent(0, entry("a", 1L)),
                           jetEvent(1, entry("b", 2L)),
                           wm(3),
                           jetEvent(3, entry("a", 3L)),
                           wm(4),
                           jetEvent(4, entry("b", 4L))
                   ));
    }

    @Test
    public void mapStateful_withTtl_surviveWm() {
        SupplierEx<Processor> supplier = createSupplier(
                2,
                jetEvent -> jetEvent.payload().getKey(),
                JetEvent::timestamp,
                () -> new long[1],
                (long[] s, Object k, JetEvent<Entry<String, Long>> e) -> {
                    s[0] += e.payload().getValue();
                    return jetEvent(e.timestamp(), entry(k, s[0]));
                },
                null,
                expandJetEventFn
        );

        TestSupport.verifyProcessor(supplier)
                .input(asList(
                        jetEvent(1, entry("b", 1L)),
                        wm(2),
                        jetEvent(2, entry("b", 2L))
                ))
                .expectOutput(asExpandedList(expandJetEventFn,
                        jetEvent(1, entry("b", 1L)),
                        wm(2),
                        jetEvent(2, entry("b", 3L))
                ));
    }

    @Test
    public void mapStateful_withTtl_evictOnlyExpired() {
        SupplierEx<Processor> supplier = createSupplier(
                2,
                jetEvent -> jetEvent.payload().getKey(),
                JetEvent::timestamp,
                () -> new long[1],
                (long[] s, Object k, JetEvent<Entry<String, Long>> e) -> {
                    s[0] += e.payload().getValue();
                    return jetEvent(e.timestamp(), entry(k, s[0]));
                },
                null,
                expandJetEventFn
        );

        TestSupport.verifyProcessor(supplier)
                .input(asList(
                        jetEvent(0, entry("a", 1L)),
                        jetEvent(1, entry("b", 2L)),
                        wm(3),
                        jetEvent(3, entry("a", 3L)),
                        jetEvent(3, entry("b", 4L))
                ))
                .expectOutput(asExpandedList(expandJetEventFn,
                        jetEvent(0, entry("a", 1L)),
                        jetEvent(1, entry("b", 2L)),
                        wm(3),
                        jetEvent(3, entry("a", 3L)),
                        jetEvent(3, entry("b", 6L))
                ));
    }

    @Test
    public void mapStateful_withTtlAndEvict() {
        long evictSignal = 99L;
        SupplierEx<Processor> supplier = createSupplier(
                2,
                jetEvent -> jetEvent.payload().getKey(),
                JetEvent::timestamp,
                () -> new long[1],
                (long[] s, Object k, JetEvent<Entry<String, Long>> e) -> {
                    s[0] += e.payload().getValue();
                    return jetEvent(e.timestamp(), entry(k, s[0]));
                },
                (state, key, wm) -> jetEvent(wm, entry(key, evictSignal)),
                expandJetEventFn
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
                   .expectOutput(asExpandedList(expandJetEventFn,
                           jetEvent(0, entry("a", 1L)),
                           jetEvent(1, entry("b", 2L)),
                           jetEvent(3, entry("a", evictSignal)),
                           wm(3),
                           jetEvent(3, entry("a", 3L)),
                           jetEvent(4, entry("b", evictSignal)),
                           wm(4),
                           jetEvent(4, entry("b", 4L)),
                           jetEvent(Long.MAX_VALUE, entry("a", 99L)),
                           jetEvent(Long.MAX_VALUE, entry("b", 99L))
                   ));
    }

    @Test
    public void mapStateful_withTtl_manyKeys() {
        SupplierEx<Processor> supplier = createSupplier(
                2,
                (JetEvent<Entry<String, Long>> jetEvent) -> jetEvent.payload().getKey(),
                JetEvent::timestamp,
                () -> new long[1],
                (long[] s, Object k, JetEvent<Entry<String, Long>> e) -> {
                    s[0] += e.payload().getValue();
                    return jetEvent(e.timestamp(), entry(k, s[0]));
                },
                null,
                expandJetEventFn
        );

        int numKeys = 100;

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
                   .expectOutput(asExpandedList(expandJetEventFn, input.toArray()));
    }

    @Test
    public void mapStateful_lateEvent() {
        SupplierEx<Processor> supplier = Processors.mapStatefulP(
                1000,
                jetEvent -> 0L,
                JetEvent::timestamp,
                () -> new long[1],
                (long[] s, Object k, JetEvent<Long> e) -> {
                    s[0] += e.payload();
                    return jetEvent(e.timestamp(), s[0]);
                },
                null
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
        SupplierEx<Processor> supplier = createSupplier(
                2,
                jetEvent -> jetEvent.payload().getKey(),
                JetEvent::timestamp,
                () -> new long[1],
                (long[] s, Object k, JetEvent<Entry<String, Long>> e) -> {
                    s[0] += e.payload().getValue();
                    return jetEvent(e.timestamp(), entry(k, s[0]));
                },
                null,
                expandJetEventFn
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
                   .expectOutput(asExpandedList(expandJetEventFn,
                           jetEvent(-10, entry("a", 1L)),
                           jetEvent(-9, entry("b", 2L)),
                           wm(-7),
                           jetEvent(-7, entry("a", 3L)),
                           jetEvent(-7, entry("b", 5L)),
                           wm(-4),
                           jetEvent(-4, entry("b", 4L))
                   ));
    }

    private <OUT> List<Object> asExpandedList(Function<OUT, Traverser<OUT>> expandFn, Object ... items) {
        if (!flatMap) {
            return asList(items);
        }
        List<Object> result = new ArrayList<>();
        for (Object item : items) {
            if (item instanceof Watermark) {
                result.add(item);
            } else {
                @SuppressWarnings("unchecked")
                Traverser<OUT> t = expandFn.apply((OUT) item);
                for (OUT r; (r = t.next()) != null; ) {
                    result.add(r);
                }
            }
        }
        return result;
    }

    private <T, K, S, R> SupplierEx<Processor> createSupplier(
            long ttl,
            @Nonnull FunctionEx<? super T, ? extends K> keyFn,
            @Nonnull ToLongFunctionEx<? super T> timestampFn,
            @Nonnull Supplier<? extends S> createFn,
            @Nonnull TriFunction<? super S, ? super K, ? super T, ? extends R> statefulMapFn,
            @Nullable TriFunction<? super S, ? super K, ? super Long, ? extends R> onEvictFn,
            @Nonnull Function<R, Traverser<R>> flatMapExpandFn
    ) {
        if (flatMap) {
            return Processors.<T, K, S, R>flatMapStatefulP(ttl, keyFn, timestampFn, createFn,
                    (s, k, t) -> {
                        R r = statefulMapFn.apply(s, k, t);
                        return r != null ? flatMapExpandFn.apply(r) : Traversers.empty();
                    },
                    onEvictFn != null
                            ? (s, k, wm) -> {
                                R r = onEvictFn.apply(s, k, wm);
                                return r != null ? flatMapExpandFn.apply(r) : Traversers.empty();
                            }
                            : null);
        } else {
            return Processors.mapStatefulP(ttl, keyFn, timestampFn, createFn, statefulMapFn, onEvictFn);
        }
    }
}
