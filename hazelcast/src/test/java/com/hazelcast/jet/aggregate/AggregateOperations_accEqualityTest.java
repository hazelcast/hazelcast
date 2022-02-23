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

package com.hazelcast.jet.aggregate;

import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Arrays;
import java.util.Collection;

import static com.hazelcast.function.FunctionEx.identity;
import static com.hazelcast.jet.aggregate.AggregateOperations.allOf;
import static com.hazelcast.jet.aggregate.AggregateOperations.averagingDouble;
import static com.hazelcast.jet.aggregate.AggregateOperations.averagingLong;
import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.aggregate.AggregateOperations.linearTrend;
import static com.hazelcast.jet.aggregate.AggregateOperations.reducing;
import static com.hazelcast.jet.aggregate.AggregateOperations.summingDouble;
import static com.hazelcast.jet.aggregate.AggregateOperations.summingLong;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AggregateOperations_accEqualityTest {

    @Parameter
    public AggregateOperation<?, ?> operation;

    @Parameters
    public static Collection<AggregateOperation<?, ?>> data() {
        return Arrays.asList(
                counting(),
                summingLong(Long::longValue),
                summingDouble(Double::doubleValue),
                averagingLong(Long::longValue),
                averagingDouble(Double::doubleValue),
                linearTrend(x -> 1L, x -> 1L),
                allOf(counting(), summingLong(Long::longValue)),
                reducing(1, identity(), (a, b) -> a, (a, b) -> a)
        );
    }

    @Test
    public void testTwoAccumulatorsEqual() {
        assertTrue("this test is not needed if deduct is not implemented", operation.deductFn() != null);

        Object accumulator1 = operation.createFn();
        Object accumulator2 = operation.createFn();

        assertEquals("two empty accumulators not equal", accumulator1, accumulator2);
    }
}
