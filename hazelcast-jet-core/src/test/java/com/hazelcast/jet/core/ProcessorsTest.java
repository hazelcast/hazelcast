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

package com.hazelcast.jet.core;

import com.hazelcast.jet.Util;
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.core.test.TestSupport;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.test.HazelcastParallelClassRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static com.hazelcast.jet.Traversers.traverseIterable;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.core.processor.Processors.aggregateByKeyP;
import static com.hazelcast.jet.core.processor.Processors.combineByKeyP;
import static com.hazelcast.jet.core.processor.Processors.combineP;
import static com.hazelcast.jet.core.processor.Processors.filterP;
import static com.hazelcast.jet.core.processor.Processors.flatMapP;
import static com.hazelcast.jet.core.processor.Processors.mapP;
import static com.hazelcast.jet.core.processor.Processors.noopP;
import static com.hazelcast.jet.function.DistributedFunctions.alwaysTrue;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
public class ProcessorsTest {

    @Test
    public void map() {
        TestSupport
                .verifyProcessor(mapP(Object::toString))
                .input(singletonList(1))
                .expectOutput(singletonList("1"));
    }

    @Test
    public void filteringWithMap() {
        TestSupport
                .verifyProcessor(mapP((Integer i) -> i > 1 ? i : null))
                .input(asList(1, 2))
                .expectOutput(singletonList(2));
    }

    @Test
    public void filter() {
        TestSupport
                .verifyProcessor(filterP(o -> o.equals(1)))
                .input(asList(1, 2, 1, 2))
                .expectOutput(asList(1, 1));
    }

    @Test
    public void flatMap() {
        TestSupport
                .verifyProcessor(flatMapP(o -> traverseIterable(asList(o + "a", o + "b"))))
                .input(singletonList(1))
                .expectOutput(asList("1a", "1b"));
    }

    @Test
    public void aggregateByKey() {
        DistributedFunction<Object, String> keyFn = Object::toString;
        TestSupport
                .verifyProcessor(aggregateByKeyP(singletonList(keyFn), aggregateToListAndString(), Util::entry))
                .disableSnapshots()
                .outputChecker(TestSupport.SAME_ITEMS_ANY_ORDER)
                .input(asList(1, 1, 2, 2))
                .expectOutput(asList(
                        entry("1", "[1, 1]"),
                        entry("2", "[2, 2]")
                ));
    }

    @Test
    public void accumulateByKey() {
        DistributedFunction<Object, String> keyFn = Object::toString;
        TestSupport
                .verifyProcessor(Processors.accumulateByKeyP(singletonList(keyFn), aggregateToListAndString()))
                .disableSnapshots()
                .input(asList(1, 1, 2, 2))
                .outputChecker(TestSupport.SAME_ITEMS_ANY_ORDER)
                .expectOutput(asList(
                        entry("1", asList(1, 1)),
                        entry("2", asList(2, 2))
                ));
    }

    @Test
    public void combineByKey() {
        TestSupport
                .verifyProcessor(combineByKeyP(aggregateToListAndString(), Util::entry))
                .disableSnapshots()
                .outputChecker(TestSupport.SAME_ITEMS_ANY_ORDER)
                .input(asList(
                        entry("1", asList(1, 2)),
                        entry("1", asList(3, 4)),
                        entry("2", asList(5, 6)),
                        entry("2", asList(7, 8))
                ))
                .expectOutput(asList(
                        entry("1", "[1, 2, 3, 4]"),
                        entry("2", "[5, 6, 7, 8]")
                ));
    }

    @Test
    public void aggregate() {
        TestSupport
                .verifyProcessor(Processors.aggregateP(aggregateToListAndString()))
                .disableSnapshots()
                .input(asList(1, 2))
                .expectOutput(singletonList("[1, 2]"));
    }

    @Test
    public void accumulate() {
        TestSupport
                .verifyProcessor(Processors.accumulateP(aggregateToListAndString()))
                .disableSnapshots()
                .input(asList(1, 2))
                .expectOutput(singletonList(asList(1, 2)));
    }

    @Test
    public void combine() {
        TestSupport
                .verifyProcessor(combineP(aggregateToListAndString()))
                .disableSnapshots()
                .input(asList(
                        singletonList(1),
                        singletonList(2)
                ))
                .expectOutput(singletonList("[1, 2]"));
    }

    @Test
    public void nonCooperative_ProcessorSupplier() {
        ProcessorSupplier cooperativeSupplier = ProcessorSupplier.of(filterP(alwaysTrue()));
        ProcessorSupplier nonCooperativeSupplier = Processors.nonCooperativeP(cooperativeSupplier);
        assertTrue(cooperativeSupplier.get(1).iterator().next().isCooperative());
        assertFalse(nonCooperativeSupplier.get(1).iterator().next().isCooperative());
    }

    @Test
    public void nonCooperative_SupplierProcessor() {
        DistributedSupplier<Processor> cooperativeSupplier = filterP(alwaysTrue());
        DistributedSupplier<Processor> nonCooperativeSupplier = Processors.nonCooperativeP(cooperativeSupplier);
        assertTrue(cooperativeSupplier.get().isCooperative());
        assertFalse(nonCooperativeSupplier.get().isCooperative());
    }

    @Test
    public void noop() {
        TestSupport
                .verifyProcessor(noopP())
                .input(Stream.generate(() -> "a").limit(100).collect(toList()))
                .expectOutput(emptyList());
    }

    private static <T> AggregateOperation1<T, List<T>, String> aggregateToListAndString() {
        return AggregateOperation
                .<List<T>>withCreate(ArrayList::new)
                .<T>andAccumulate(List::add)
                .andCombine(List::addAll)
                .andFinish(Object::toString);
    }
}
