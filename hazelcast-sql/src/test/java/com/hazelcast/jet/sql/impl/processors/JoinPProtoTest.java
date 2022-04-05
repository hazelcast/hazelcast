/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.processors;

import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.test.TestSupport;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static java.util.Arrays.asList;

@Category({QuickTest.class, ParallelJVMTest.class})
@RunWith(HazelcastSerialClassRunner.class)
public class JoinPProtoTest extends SimpleTestInClusterSupport {
    @Test
    public void simpleTest() {
        SupplierEx<Processor> supplier = JoinPProto::new;
        TestSupport.verifyProcessor(supplier)
                .disableSnapshots()
                .inputs(asList(
                        asList(
                                0L,
                                1L,
                                wm((byte) 0, 1L)
                        ),
                        asList(
                                0L,
                                1L,
                                wm((byte) 1, 1L)
                        )
                ))
                .expectOutput(
                        asList(
                                // left <- 0
                                // right <- 0
                                Tuple2.tuple2(0L, 0L),
                                // left <- 1
                                Tuple2.tuple2(1L, 0L),
                                // right <- 1
                                Tuple2.tuple2(0L, 1L),
                                Tuple2.tuple2(1L, 1L),
                                wm((byte) 0, 1L),
                                wm((byte) 1, 1L)
                        )
                );
    }

    @Test
    public void testWatermarkCleanup() {
        SupplierEx<Processor> supplier = JoinPProto::new;
        TestSupport.verifyProcessor(supplier)
                .disableSnapshots()
                .inputs(asList(
                        asList(
                                0L,
                                1L,
                                wm((byte) 0, 1L),
                                wm((byte) 0, 3L),
                                5L,
                                6L,
                                wm((byte) 0, 6L)
                        ),
                        asList(
                                0L,
                                1L,
                                wm((byte) 1, 1L),
                                wm((byte) 1, 3L),
                                5L,
                                6L,
                                wm((byte) 1, 6L)
                        )
                ))
                .expectOutput(
                        asList(
                                // left <- 0
                                // right <- 0
                                Tuple2.tuple2(0L, 0L),
                                // left <- 1
                                Tuple2.tuple2(1L, 0L),
                                // right <- 1
                                Tuple2.tuple2(0L, 1L),
                                Tuple2.tuple2(1L, 1L),
                                wm((byte) 0, 1L),
                                wm((byte) 1, 1L),
                                wm((byte) 0, 3L),
                                wm((byte) 1, 3L),
                                // left <- 5
                                // right <- 5
                                Tuple2.tuple2(5L, 5L),
                                // left <- 6
                                Tuple2.tuple2(6L, 5L),
                                // right <- 6
                                Tuple2.tuple2(5L, 6L),
                                Tuple2.tuple2(6L, 6L),
                                wm((byte) 0, 6L),
                                wm((byte) 1, 6L)
                        )
                );
    }
}
