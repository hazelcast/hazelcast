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

package com.hazelcast.jet.pipeline.test;

import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.pipeline.PipelineTestSupport;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.hazelcast.function.Functions.wholeItem;
import static com.hazelcast.jet.Traversers.traverseArray;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.impl.util.Util.toList;
import static com.hazelcast.jet.pipeline.test.Assertions.assertCollected;
import static com.hazelcast.jet.pipeline.test.Assertions.assertCollectedEventually;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Category({QuickTest.class, ParallelJVMTest.class})
public class AssertionsTest extends PipelineTestSupport {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void test_assertOrdered() {
        p.readFrom(TestSources.items(1, 2, 3, 4))
         .apply(Assertions.assertOrdered(Arrays.asList(1, 2, 3, 4)));

        execute();
    }

    @Test
    public void test_assertOrdered_should_fail() throws Throwable {
        p.readFrom(TestSources.items(4, 3, 2, 1))
         .apply(Assertions.assertOrdered(Arrays.asList(1, 2, 3, 4)));

        expectedException.expectMessage(AssertionError.class.getName());
        executeAndPeel();
    }

    @Test
    public void test_assertOrdered_not_terminal() {
        List<Integer> assertionSink = hz().getList(sinkName);
        assertTrue(assertionSink.isEmpty());

        p.readFrom(TestSources.items(1, 2, 3, 4))
                .apply(Assertions.assertOrdered(Arrays.asList(1, 2, 3, 4)))
                .writeTo(sinkList());

        execute();

        assertEquals(4, assertionSink.size());
        assertEquals(1, (int) assertionSink.get(0));
        assertEquals(2, (int) assertionSink.get(1));
        assertEquals(3, (int) assertionSink.get(2));
        assertEquals(4, (int) assertionSink.get(3));
    }

    @Test
    public void test_assertOrdered_not_terminal_should_fail() throws Throwable {
        List<Integer> assertionSink = hz().getList(sinkName);
        assertTrue(assertionSink.isEmpty());

        p.readFrom(TestSources.items(1, 2, 4, 3))
                .apply(Assertions.assertOrdered(Arrays.asList(1, 2, 3, 4)))
                .writeTo(sinkList());

        expectedException.expectMessage(AssertionError.class.getName());
        executeAndPeel();

        assertEquals(4, assertionSink.size());
        assertEquals(1, (int) assertionSink.get(0));
        assertEquals(2, (int) assertionSink.get(1));
    }

    @Test
    public void test_assertAnyOrder() {
        p.readFrom(TestSources.items(4, 3, 2, 1))
         .apply(Assertions.assertAnyOrder(Arrays.asList(1, 2, 3, 4)));

        execute();
    }

    @Test
    public void test_assertAnyOrder_distributedSource() {
        List<Integer> input = IntStream.range(0, itemCount).boxed().collect(Collectors.toList());
        putToBatchSrcMap(input);

        List<Entry<String, Integer>> expected = toList(input, i -> entry(String.valueOf(i), i));

        p.readFrom(Sources.map(srcMap))
         .apply(Assertions.assertAnyOrder(expected));

        execute();
    }

    @Test
    public void test_assertAnyOrder_should_fail() throws Throwable {
        p.readFrom(TestSources.items(3, 2, 1))
         .apply(Assertions.assertAnyOrder(Arrays.asList(1, 2, 3, 4)));

        expectedException.expectMessage(AssertionError.class.getName());
        executeAndPeel();
    }

    @Test
    public void test_assertAnyOrder_duplicate_entry() throws Throwable {
        p.readFrom(TestSources.items(1, 3, 2, 3))
                .apply(Assertions.assertAnyOrder(Arrays.asList(1, 2, 3, 3)));

        execute();
    }

    @Test
    public void test_assertAnyOrder_duplicate_entry_only_in_source_should_fail() throws Throwable {
        p.readFrom(TestSources.items(1, 3, 2, 3))
                .apply(Assertions.assertAnyOrder(Arrays.asList(1, 2, 3)));

        expectedException.expectMessage(AssertionError.class.getName());
        executeAndPeel();
    }

    @Test
    public void test_assertAnyOrder_duplicate_entry_only_in_assert_should_fail() throws Throwable {
        p.readFrom(TestSources.items(1, 2, 3))
                .apply(Assertions.assertAnyOrder(Arrays.asList(1, 2, 3, 3)));

        expectedException.expectMessage(AssertionError.class.getName());
        executeAndPeel();
    }

    @Test
    public void test_assertAnyOrder_not_terminal() {
        List<Integer> assertionSink = hz().getList(sinkName);
        assertTrue(assertionSink.isEmpty());

        p.readFrom(TestSources.items(4, 3, 2, 1))
                .apply(Assertions.assertAnyOrder(Arrays.asList(1, 2, 3, 4)))
                .writeTo(sinkList());

        execute();

        assertEquals(4, assertionSink.size());
        assertEquals(4, (int) assertionSink.get(0));
        assertEquals(3, (int) assertionSink.get(1));
        assertEquals(2, (int) assertionSink.get(2));
        assertEquals(1, (int) assertionSink.get(3));
    }

    @Test
    public void test_assertAnyOrder_not_terminal_should_fail() throws Throwable {
        List<Integer> assertionSink = hz().getList(sinkName);
        assertTrue(assertionSink.isEmpty());

        p.readFrom(TestSources.items(3, 2, 1))
                .apply(Assertions.assertAnyOrder(Arrays.asList(1, 2, 3, 4)))
                .writeTo(sinkList());

        expectedException.expectMessage(AssertionError.class.getName());
        executeAndPeel();

        assertEquals(4, assertionSink.size());
        assertEquals(3, (int) assertionSink.get(0));
        assertEquals(2, (int) assertionSink.get(1));
        assertEquals(1, (int) assertionSink.get(2));
    }

    @Test
    public void test_assertContains() {
        p.readFrom(TestSources.items(4, 3, 2, 1))
         .apply(Assertions.assertContains(Arrays.asList(1, 3)));

        execute();
    }

    @Test
    public void test_assertContains_should_fail() throws Throwable {
        p.readFrom(TestSources.items(4, 1, 2, 3))
         .apply(Assertions.assertContains(Arrays.asList(1, 3, 5)));

        expectedException.expectMessage(AssertionError.class.getName());
        executeAndPeel();
    }

    @Test
    public void test_assertContains_not_terminal() {
        List<Integer> assertionSink = hz().getList(sinkName);
        assertTrue(assertionSink.isEmpty());

        p.readFrom(TestSources.items(4, 3, 2, 1))
                .apply(Assertions.assertContains(Arrays.asList(1, 3)))
                .writeTo(sinkList());

        execute();

        assertEquals(4, assertionSink.size());
        assertEquals(4, (int) assertionSink.get(0));
        assertEquals(3, (int) assertionSink.get(1));
        assertEquals(2, (int) assertionSink.get(2));
        assertEquals(1, (int) assertionSink.get(3));
    }

    @Test
    public void test_assertContains_not_terminal_should_fail() throws Throwable {
        List<Integer> assertionSink = hz().getList(sinkName);
        assertTrue(assertionSink.isEmpty());

        p.readFrom(TestSources.items(4, 1, 2, 3))
                .apply(Assertions.assertContains(Arrays.asList(1, 3, 5)))
                .writeTo(sinkList());

        expectedException.expectMessage(AssertionError.class.getName());
        executeAndPeel();

        assertEquals(4, assertionSink.size());
        assertEquals(4, (int) assertionSink.get(0));
        assertEquals(1, (int) assertionSink.get(1));
        assertEquals(2, (int) assertionSink.get(2));
        assertEquals(3, (int) assertionSink.get(3));
    }

    @Test
    public void test_assertCollected() {
        p.readFrom(TestSources.items(4, 3, 2, 1))
         .apply(assertCollected(c -> assertTrue("list size must be at least 4", c.size() >= 4)));

        execute();
    }

    @Test
    public void test_assertCollected_should_fail() throws Throwable {
        p.readFrom(TestSources.items(1))
         .apply(assertCollected(c -> assertTrue("list size must be at least 4", c.size() >= 4)));

        expectedException.expectMessage(AssertionError.class.getName());
        executeAndPeel();
    }

    @Test
    public void test_assertCollected_not_terminal() {
        List<Integer> assertionSink = hz().getList(sinkName);
        assertTrue(assertionSink.isEmpty());

        p.readFrom(TestSources.items(4, 3, 2, 1))
                .apply(assertCollected(c -> assertTrue("list size must be at least 4", c.size() >= 4)))
                .writeTo(sinkList());

        execute();

        assertEquals(4, assertionSink.size());
        assertEquals(4, (int) assertionSink.get(0));
        assertEquals(3, (int) assertionSink.get(1));
        assertEquals(2, (int) assertionSink.get(2));
        assertEquals(1, (int) assertionSink.get(3));
    }

    @Test
    public void test_assertCollected_not_terminal_should_fail() throws Throwable {
        List<Integer> assertionSink = hz().getList(sinkName);
        assertTrue(assertionSink.isEmpty());

        p.readFrom(TestSources.items(1))
                .apply(assertCollected(c -> assertTrue("list size must be at least 4", c.size() >= 4)))
                .writeTo(sinkList());

        expectedException.expectMessage(AssertionError.class.getName());
        executeAndPeel();

        assertEquals(1, assertionSink.size());
        assertEquals(1, (int) assertionSink.get(0));
    }

    @Test
    public void test_assertCollectedEventually() throws Throwable {
        p.readFrom(TestSources.itemStream(1, (ts, seq) -> 0L))
         .withoutTimestamps()
         .apply(assertCollectedEventually(5, c -> assertTrue("did not receive item '0'", c.contains(0L))));

        expectedException.expectMessage(AssertionCompletedException.class.getName());
        executeAndPeel();
    }

    @Test
    public void test_assertCollectedEventually_should_fail() throws Throwable {
        p.readFrom(TestSources.itemStream(1, (ts, seq) -> 0L))
         .withoutTimestamps()
         .apply(assertCollectedEventually(5, c -> assertTrue("did not receive item '1'", c.contains(1L))));

        expectedException.expectMessage(AssertionError.class.getName());
        executeAndPeel();
    }

    @Test
    public void test_assertCollectedEventuallynot_terminal() throws Throwable {
        List<Integer> assertionSink = hz().getList(sinkName);
        assertTrue(assertionSink.isEmpty());

        p.readFrom(TestSources.itemStream(1, (ts, seq) -> 0L))
                .withoutTimestamps()
                .apply(assertCollectedEventually(5, c -> assertTrue("did not receive item '0'", c.contains(0L))))
                .writeTo(sinkList());

        expectedException.expectMessage(AssertionCompletedException.class.getName());
        executeAndPeel();

        assertFalse(assertionSink.isEmpty());
    }

    @Test
    public void test_assertCollectedEventuallynot_terminal_should_fail() throws Throwable {
        List<Integer> assertionSink = hz().getList(sinkName);
        assertTrue(assertionSink.isEmpty());

        p.readFrom(TestSources.itemStream(1, (ts, seq) -> 0L))
                .withoutTimestamps()
                .apply(assertCollectedEventually(5, c -> assertTrue("did not receive item '1'", c.contains(1L))))
                .writeTo(sinkList());

        expectedException.expectMessage(AssertionError.class.getName());
        executeAndPeel();

        assertFalse(assertionSink.isEmpty());
    }

    @Test
    public void test_multiple_assertions_in_pipeline() throws Throwable {
        Map<String, Long> assertionSink = hz().getMap(sinkName);
        assertTrue(assertionSink.isEmpty());

        p.readFrom(TestSources.items("some text here and here and some here"))
                .apply(Assertions.assertOrdered(Arrays.asList("some text here and here and some here")))
                .flatMap(line -> traverseArray(line.toLowerCase(Locale.ROOT).split("\\W+")))
                .apply(Assertions.assertAnyOrder(
                        Arrays.asList("some", "text", "here", "and", "here", "and", "some", "here")))
                .filter(word -> !word.equals("and"))
                .apply(Assertions.assertContains(Arrays.asList("some", "text")))
                .apply(Assertions.assertContains(Arrays.asList("some", "here")))
                .groupingKey(wholeItem())
                .aggregate(AggregateOperations.counting())
                .writeTo(Sinks.map(sinkName));

        execute();

        assertEquals(3, assertionSink.size());
        assertEquals(2, (long) assertionSink.get("some"));
        assertEquals(1, (long) assertionSink.get("text"));
        assertEquals(3, (long) assertionSink.get("here"));
    }
}
