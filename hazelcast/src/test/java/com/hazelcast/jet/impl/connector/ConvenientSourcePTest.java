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

package com.hazelcast.jet.impl.connector;

import com.hazelcast.internal.util.MutableInteger;
import com.hazelcast.jet.core.test.TestSupport;
import com.hazelcast.jet.impl.pipeline.transform.StreamSourceTransform;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import static com.hazelcast.jet.core.EventTimePolicy.noEventTime;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ConvenientSourcePTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void test_faultTolerance() {
        long totalCount = 5L;
        StreamSource<Integer> source = SourceBuilder
                .stream("src", ctx -> new MutableInteger())
                .<Integer>fillBufferFn((src, buffer) -> {
                    while (src.value < totalCount) {
                        buffer.add(src.getAndInc());
                    }
                })
                .createSnapshotFn(src -> src.value)
                .restoreSnapshotFn((src, states) -> {
                    assert states.size() == 1;
                    src.value = states.get(0);
                })
                .distributed(1) // we use this to avoid forceTotalParallelismOne
                .build();

        TestSupport
                .verifyProcessor(((StreamSourceTransform<Integer>) source).metaSupplierFn.apply(noEventTime()))
                .runUntilOutputMatches(10_000, 0)
                .disableProgressAssertion()
                .expectOutput(Arrays.asList(0, 1, 2, 3, 4));
    }

    @Test
    public void test_nullState() {
        StreamSource<String> source = SourceBuilder
                .stream("src", ctx -> new boolean[1])
                .<String>fillBufferFn((src, buffer) -> {
                    if (!src[0]) {
                        buffer.add("item after restore");
                        src[0] = true;
                    }
                })
                .createSnapshotFn(src -> null)
                .restoreSnapshotFn((src, states) -> fail("should not get here"))
                .distributed(1) // we use this to avoid forceTotalParallelismOne
                .build();

        TestSupport
                .verifyProcessor(((StreamSourceTransform<String>) source).metaSupplierFn.apply(noEventTime()))
                .runUntilOutputMatches(10_000, 0)
                .disableProgressAssertion()
                .outputChecker((e, a) -> new HashSet<>(e).equals(new HashSet<>(a)))
                .expectOutput(Collections.singletonList("item after restore"));
    }

    @Test
    public void when_streamingSourceClosesBuffer_then_fails() {
        StreamSource<Integer> source = SourceBuilder
                .stream("src", ctx -> null)
                .<Integer>fillBufferFn((src, buffer) -> buffer.close())
                .distributed(1) // we use this to avoid forceTotalParallelismOne
                .build();

        exception.expectMessage("streaming source must not close the buffer");
        TestSupport
                .verifyProcessor(((StreamSourceTransform<Integer>) source).metaSupplierFn.apply(noEventTime()))
                .expectOutput(Collections.emptyList());
    }
}
