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

package com.hazelcast.jet.pipeline.test;

import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.pipeline.PipelineTestSupport;
import com.hazelcast.jet.pipeline.WindowDefinition;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import static com.hazelcast.jet.pipeline.test.Assertions.assertCollectedEventually;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestSourcesTest extends PipelineTestSupport {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void test_items() {
        Object[] input = IntStream.range(0, 10_000).boxed().toArray();

        List<Object> expected = Arrays.asList(input);

        p.drawFrom(TestSources.items(input))
         .apply(Assertions.assertOrdered(expected));

        jet().newJob(p).join();
    }

    @Test
    public void test_itemStream() throws Throwable {
        int expectedItemCount = 20;

        p.drawFrom(TestSources.itemStream(10))
         .withoutTimestamps()
         .apply(assertCollectedEventually(10, items -> {
             assertTrue("list should contain at least " + expectedItemCount + " items", items.size() > expectedItemCount);
             for (int i = 0; i < items.size(); i++) {
                 assertEquals(i, items.get(i).sequence());
             }
         }));

        expectedException.expect(AssertionCompletedException.class);
        executeAndPeel();

    }

    @Test
    public void test_itemStream_withWindowing() throws Throwable {
        int itemsPerSecond = 10;

        p.drawFrom(TestSources.itemStream(itemsPerSecond))
         .withNativeTimestamps(0)
         .window(WindowDefinition.tumbling(1000))
         .aggregate(AggregateOperations.counting())
         .apply(assertCollectedEventually(10, windowResults -> {
             assertTrue("sink list should contain some items", windowResults.size() > 1);
             // first window may be incomplete, subsequent windows should have 10 items
             assertEquals(10L, (long) windowResults.get(1).result());
         }));

        expectedException.expect(AssertionCompletedException.class);
        executeAndPeel();
    }
}
