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

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.datamodel.Tuple3;
import com.hazelcast.jet.function.SupplierEx;
import com.hazelcast.jet.function.TriFunction;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

import static com.hazelcast.jet.core.test.TestSupport.verifyProcessor;
import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.jet.datamodel.Tuple3.tuple3;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

@Category(ParallelTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class HashJoinPTest extends JetTestSupport {

    private final BiFunction mapToOutputBi = Tuple2::tuple2;
    private final TriFunction mapToOutputTri = Tuple3::tuple3;
    private JetInstance instance;

    @Before
    public void setUp() {
        instance = this.createJetMember();
    }

    @Test
    public void when_oneToOneJoinBiJoin_then_producesCorrectOutput() {
        Function<Integer, Object> enrichingSideKeyFn = e -> e;
        List<List<?>> inputs = asList(
                asList(1, 2, 3),
                hashJoinCollected(e -> e, 1, 3)
        );
        List<Tuple2<Integer, Integer>> expectedOutput = asList(
                tuple2(1, 1),
                tuple2(3, 3)
        );

        testJoin(asList(e -> e, enrichingSideKeyFn), inputs, expectedOutput);
    }
    @Test
    public void when_oneToManyJoinBiJoin_then_producesCorrectOutput() {
        Function<Integer, Object> enrichingSideKeyFn = e -> e % 10;

        List<List<?>> inputs = asList(
                asList(1, 2, 3), //
                hashJoinCollected(enrichingSideKeyFn, 1, 3, 11, 12)
        );
        List<Tuple2<Integer, Integer>> expectedOutput = asList(
                tuple2(1, 1),
                tuple2(1, 11),
                tuple2(2, 12),
                tuple2(3, 3)
        );

        testJoin(asList(e -> e, enrichingSideKeyFn), inputs, expectedOutput);
    }

    @Test
    public void when_oneToOneJoinTriJoin_then_producesCorrectOutput() {
        Function<Integer, Object> keyFn = e -> e;
        List<List<?>> inputs = asList(
                asList(1, 2, 3), //
                hashJoinCollected(e -> e, 1, 3), //
                hashJoinCollected(e -> e, 1, 3)
        );
        List<Tuple3<Integer, Integer, Integer>> expectedOutput = asList(
                tuple3(1, 1, 1), //
                tuple3(3, 3, 3)
        );

        testJoin(asList(keyFn, keyFn, keyFn), inputs, expectedOutput);
    }

    private void testJoin(
            List<Function<Integer, Object>> keyFunctions,
            List<List<?>> inputs,
            List<?> expectedOutput
    ) {
        SupplierEx<Processor> supplier = () -> new HashJoinP<>(
                keyFunctions,
                emptyList(),
                mapToOutputBi,
                mapToOutputTri
        );

        int[] priorities = inputs.size() == 2 ? new int[]{ 10, 1 } : new int[]{ 10, 1, 1 };
        verifyProcessor(supplier)
                .jetInstance(instance)
                .disableSnapshots()
                .disableLogging()
                .inputs(inputs, priorities)
                .expectOutput(expectedOutput);
    }

    /**
     * Second and third ordinal has one element list of Map as an input.
     * This function simulates output of {@link HashJoinCollectP}
     */
    @SafeVarargs
    private static <E0> List<Map<Object, List<E0>>> hashJoinCollected(Function<E0, Object> keyFn, E0... items) {
        Map<Object, List<E0>> map = new HashMap<>();
        for (E0 item : items) {
            map.computeIfAbsent(keyFn.apply(item), k -> new ArrayList<>())
                    .add(item);
        }
        return singletonList(map);
    }

}
