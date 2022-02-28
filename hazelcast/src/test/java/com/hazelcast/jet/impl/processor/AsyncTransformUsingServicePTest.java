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

package com.hazelcast.jet.impl.processor;

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.core.test.TestInbox;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import com.hazelcast.jet.core.test.TestSupport;
import com.hazelcast.jet.pipeline.ServiceFactories;
import com.hazelcast.jet.pipeline.ServiceFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.jet.Traversers.traverseItems;
import static com.hazelcast.jet.impl.util.Util.exceptionallyCompletedFuture;
import static com.hazelcast.jet.pipeline.GeneralStage.DEFAULT_MAX_CONCURRENT_OPS;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParametrizedRunner.class)
@Parameterized.UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AsyncTransformUsingServicePTest extends SimpleTestInClusterSupport {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Parameter
    public boolean ordered;

    @Parameters(name = "ordered={0}")
    public static Collection<Object> parameters() {
        return asList(true, false);
    }

    private ProcessorSupplier getSupplier(
            BiFunctionEx<? super String, ? super String, CompletableFuture<Traverser<String>>> mapFn
    ) {
        return getSupplier(DEFAULT_MAX_CONCURRENT_OPS, mapFn);
    }

    private ProcessorSupplier getSupplier(
            int maxConcurrentOps,
            BiFunctionEx<? super String, ? super String, CompletableFuture<Traverser<String>>> mapFn
    ) {
        ServiceFactory<?, String> serviceFactory = ServiceFactories
                .nonSharedService(pctx -> "foo");
        return ordered
                ? AsyncTransformUsingServiceOrderedP.supplier(
                        serviceFactory, maxConcurrentOps, mapFn)
                : AsyncTransformUsingServiceUnorderedP.supplier(
                        serviceFactory, maxConcurrentOps, mapFn, FunctionEx.identity());
    }

    @BeforeClass
    public static void setUp() {
        initialize(1, null);
    }

    @Test
    public void test_completedFutures() {
        TestSupport
                .verifyProcessor(getSupplier((ctx, item) -> completedFuture(traverseItems(item + "-1", item + "-2"))))
                .hazelcastInstance(instance())
                .input(asList("a", "b"))
                .outputChecker((expected, actual) ->
                        actual.equals(asList("a-1", "a-2", "b-1", "b-2"))
                                || !ordered && actual.equals(asList("b-1", "b-2", "a-1", "a-2")))
                .disableProgressAssertion()
                .expectOutput(singletonList("<see code>"));
    }

    @Test
    public void test_futuresCompletedInSeparateThread() {
        TestSupport
                .verifyProcessor(getSupplier((ctx, item) -> {
                            CompletableFuture<Traverser<String>> f = new CompletableFuture<>();
                            spawn(() -> f.complete(traverseItems(item + "-1", item + "-2")));
                            return f;
                        })
                )
                .hazelcastInstance(instance())
                .input(asList("a", "b", new Watermark(10)))
                .outputChecker((expected, actual) ->
                        actual.equals(asList("a-1", "a-2", "b-1", "b-2", wm(10)))
                                || !ordered && actual.equals(asList("b-1", "b-2", "a-1", "a-2", wm(10))))
                .disableProgressAssertion()
                .expectOutput(singletonList("<see code>"));
    }

    @Test
    public void test_forwardWatermarksWithoutItems() {
        TestSupport
                .verifyProcessor(getSupplier((ctx, item) -> {
                    throw new UnsupportedOperationException();
                }))
                .hazelcastInstance(instance())
                .input(singletonList(wm(10)))
                .expectOutput(singletonList(wm(10)));
    }

    @Test
    public void when_mapFnReturnsNullFuture_then_filteredOut() {
        TestSupport
                .verifyProcessor(getSupplier((ctx, item) -> null))
                .hazelcastInstance(instance())
                .input(asList("a", "b"))
                .expectOutput(emptyList());
    }

    @Test
    public void when_futureReturnsNullTraverser_then_resultFilteredOut() {
        TestSupport
                .verifyProcessor(getSupplier((ctx, item) -> null))
                .hazelcastInstance(instance())
                .input(singletonList(wm(10)))
                .expectOutput(singletonList(wm(10)));
    }

    @Test
    public void when_futureCompletedExceptionally_then_jobFails() {
        exception.expect(JetException.class);
        exception.expectMessage("test exception");
        TestSupport
                .verifyProcessor(getSupplier((ctx, item) ->
                        exceptionallyCompletedFuture(new RuntimeException("test exception"))))
                .hazelcastInstance(instance())
                .input(singletonList("a"))
                .expectOutput(emptyList());
    }

    @Test
    public void test_wmNotCountedToParallelOps() throws Exception {
        Processor processor = getSupplier(
                2, (ctx, item) -> new CompletableFuture<>()).get(1).iterator().next();
        processor.init(new TestOutbox(128), new TestProcessorContext());
        TestInbox inbox = new TestInbox();
        inbox.add("foo");
        processor.process(0, inbox);
        assertTrue("inbox not empty", inbox.isEmpty());
        assertTrue("wm rejected", processor.tryProcessWatermark(wm(0)));
        inbox.add("bar");
        processor.process(0, inbox);
        assertTrue("2nd item rejected even though max parallel ops is 1", inbox.isEmpty());
    }

    @Test
    public void test_watermarksConflated() throws Exception {
        Processor processor = getSupplier(
                2, (ctx, item) -> new CompletableFuture<>()).get(1).iterator().next();
        processor.init(new TestOutbox(128), new TestProcessorContext());
        TestInbox inbox = new TestInbox();
        // i have to add an item first because otherwise the WMs are forwarded right away
        inbox.add("foo");
        processor.process(0, inbox);
        assertTrue("inbox not empty", inbox.isEmpty());
        assertTrue("wm rejected", processor.tryProcessWatermark(wm(0)));
        assertTrue("wm rejected", processor.tryProcessWatermark(wm(1)));
        assertTrue("wm rejected", processor.tryProcessWatermark(wm(2)));
    }
}
