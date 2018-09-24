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

import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.core.test.TestSupport;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map.Entry;

import static com.hazelcast.jet.Util.entry;
import static java.util.Arrays.asList;

@Category(ParallelTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class RollingAggregatePTest {

    @Test
    public void rollingAggregate() {
        DistributedSupplier<Processor> supplier = Processors.rollingAggregateP(
                Entry::getKey,
                AggregateOperation
                        .withCreate(() -> new long[1])
                        .<Entry<String, Long>>andAccumulate((acc, t) -> acc[0] += t.getValue())
                        .andExportFinish(acc -> acc[0]),
                (item, key, result) -> entry(key, result));

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
    public void rollingAggregate_withFiltering() {
        DistributedSupplier<Processor> supplier = Processors.rollingAggregateP(
                Entry::getKey,
                AggregateOperation
                        .withCreate(() -> new long[1])
                        .<Entry<String, Long>>andAccumulate((acc, t) -> acc[0] += t.getValue())
                        .andExportFinish(acc -> acc[0] > 2 ? acc[0] : null),
                (item, key, result) -> result == null ? null : entry(key, result));

        TestSupport.verifyProcessor(supplier)
                .input(asList(
                        entry("a", 1L),
                        entry("b", 2L),
                        entry("a", 3L),
                        entry("b", 4L)
                ))
                .expectOutput(asList(
                        entry("a", 4L),
                        entry("b", 6L)
                ));
    }
}
