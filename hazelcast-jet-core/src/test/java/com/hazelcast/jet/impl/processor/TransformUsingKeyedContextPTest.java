/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.core.test.TestSupport;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.pipeline.ContextFactory;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map.Entry;
import java.util.stream.LongStream;

import static com.hazelcast.jet.Traversers.traverseStream;
import static com.hazelcast.jet.Util.entry;
import static java.util.Arrays.asList;

@Category(ParallelTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class TransformUsingKeyedContextPTest {

    @Test
    public void test_map() {
        DistributedSupplier<Processor> supplier =
                Processors.mapUsingKeyedContextP(
                        ContextFactory.withCreateFn(jet -> new long[1]),
                        (Entry<String, Long> t) -> t.getKey(),
                        (ctx, item) -> entry(item.getKey(), ctx[0] += item.getValue())
                );

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
    public void test_filterUsingMap() {
        DistributedSupplier<Processor> supplier =
                Processors.mapUsingKeyedContextP(
                        ContextFactory.withCreateFn(jet -> new long[1]),
                        (Entry<String, Long> t) -> t.getKey(),
                        (ctx, item) -> (ctx[0] += item.getValue()) > 2 ? item : null
                );

        TestSupport.verifyProcessor(supplier)
                   .input(asList(
                           entry("a", 1L),
                           entry("b", 2L),
                           entry("a", 3L),
                           entry("b", 4L)
                   ))
                   .expectOutput(asList(
                           entry("a", 3L),
                           entry("b", 4L)
                   ));
    }

    @Test
    public void test_filter() {
        DistributedSupplier<Processor> supplier =
                Processors.filterUsingKeyedContextP(
                        ContextFactory.withCreateFn(jet -> new long[1]),
                        (Entry<String, Long> t) -> t.getKey(),
                        (ctx, item) -> (ctx[0] += item.getValue()) > 2
                );

        TestSupport.verifyProcessor(supplier)
                   .input(asList(
                           entry("a", 1L),
                           entry("b", 2L),
                           entry("a", 3L),
                           entry("b", 4L)
                   ))
                   .expectOutput(asList(
                           entry("a", 3L),
                           entry("b", 4L)
                   ));
    }

    @Test
    public void test_flatMap() {
        DistributedSupplier<Processor> supplier =
                Processors.flatMapUsingKeyedContextP(
                        ContextFactory.withCreateFn(jet -> new long[1]),
                        (Entry<String, Long> t) -> t.getKey(),
                        (ctx, item) -> traverseStream(LongStream.range(0, item.getValue())
                                                                .mapToObj(i -> entry(item.getKey(), i)))
                );

        TestSupport.verifyProcessor(supplier)
                   .input(asList(
                           entry("a", 1L),
                           entry("b", 2L),
                           entry("a", 3L),
                           entry("b", 4L)
                   ))
                   .expectOutput(asList(
                           entry("a", 0L),
                           entry("b", 0L),
                           entry("b", 1L),
                           entry("a", 0L),
                           entry("a", 1L),
                           entry("a", 2L),
                           entry("b", 0L),
                           entry("b", 1L),
                           entry("b", 2L),
                           entry("b", 3L)
                   ));
    }
}
