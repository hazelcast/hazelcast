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

        p.readFrom(TestSources.items(input))
                .apply(Assertions.assertOrdered(expected));

        jet().newJob(p).join();
    }

    @Test
    public void test_itemStream() throws Throwable {
        int expectedItemCount = 20;

        p.readFrom(TestSources.itemStream(10))
         .withoutTimestamps()
         .apply(assertCollectedEventually(10, items -> {
             assertTrue("list should contain at least " + expectedItemCount + " items", items.size() > expectedItemCount);
             for (int i = 0; i < items.size(); i++) {
                 assertEquals(i, items.get(i).sequence());
             }
         }));

        expectedException.expectMessage(AssertionCompletedException.class.getName());
        executeAndPeel();

    }

    @Test
    public void test_itemStream_withWindowing() throws Throwable {
        int itemsPerSecond = 10;

        p.readFrom(TestSources.itemStream(itemsPerSecond))
         .withNativeTimestamps(0)
         .window(WindowDefinition.tumbling(1000))
         .aggregate(AggregateOperations.counting())
         .apply(assertCollectedEventually(10, windowResults -> {
             // find any window that has 10 items, some may be incomplete due to hiccups
             boolean matched = windowResults.stream().anyMatch(r -> r.result() == itemsPerSecond);
             assertTrue("Did not find any window with 10 items: " + windowResults, matched);
         }));

        expectedException.expectMessage(AssertionCompletedException.class.getName());
        executeAndPeel();
    }

    /**
     * Emitted items from itemStream method are not 100% accurate (due to rounding) for numbers which does not satisfy
     * "1_000_000_000 % itemsPerSecond = 0". Goal of this test is to validate that the number of emitted items is in range
     * <0.5*expected,2*expected>, i.e. whether number of emitted items in not completely incorrect.
     */
    @Test
    public void test_itemStream_in_expected_range() throws Throwable {
        int itemsPerSecond = 5327;

        p.readFrom(TestSources.itemStream(itemsPerSecond))
                .withNativeTimestamps(0)
                .window(WindowDefinition.tumbling(1000))
                .aggregate(AggregateOperations.counting())
                .apply(assertCollectedEventually(10, windowResults -> {
                    // first window may be incomplete
                    assertTrue("sink list should contain some items", windowResults.size() > 1);
                    assertTrue("emitted items is more than twice lower then expected itemsPerSecond",
                            (long) windowResults.get(1).result() > itemsPerSecond / 2);
                    assertTrue("emitted items is more than twice higher then expected itemsPerSecond",
                            (long) windowResults.get(1).result() < itemsPerSecond * 2);
                }));

        expectedException.expectMessage(AssertionCompletedException.class.getName());
        executeAndPeel();
    }

}
