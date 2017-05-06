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

package com.hazelcast.jet.windowing;

import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Arrays;
import java.util.Collection;

import static com.hazelcast.jet.windowing.WindowOperations.counting;
import static com.hazelcast.jet.windowing.WindowOperations.linearTrend;
import static com.hazelcast.jet.windowing.WindowOperations.reducing;
import static com.hazelcast.jet.windowing.WindowOperations.summingToLong;
import static org.junit.Assert.assertEquals;

@Category(QuickTest.class)
@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
public class WindowOperations_accEqualityTest {

    @Parameter
    public WindowOperation<?, ?, ?> operation;

    @Parameters
    public static Collection<WindowOperation<?, ?, ?>> data() {
        return Arrays.asList(
                counting(),
                summingToLong(),
                linearTrend(x -> 1L, x -> 1L),
                reducing(1, null, null, null)
        );
    }

    @Test
    public void testTwoAccumulatorsEqual() {
        Object accumulator1 = operation.createAccumulatorF();
        Object accumulator2 = operation.createAccumulatorF();

        assertEquals("two empty accumulators not equal", accumulator1, accumulator2);
    }
}
