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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class WindowOperationsTest {

    @Parameter
    public WindowOperation<?, ?, ?> operation;

    @Parameters
    public static Collection<WindowOperation<?, ?, ?>> data() {
        return Arrays.asList(
                WindowOperations.counting(),
                WindowOperations.summingToLong(),
                WindowOperations.reducing(1, null, null, null)
        );
    }

    @Test
    public void testTwoAccumulatorsEqual() {
        Object accumulator1 = operation.createAccumulatorF();
        Object accumulator2 = operation.createAccumulatorF();

        assertEquals("two empty accumulators not equal", accumulator1, accumulator2);
    }

}
