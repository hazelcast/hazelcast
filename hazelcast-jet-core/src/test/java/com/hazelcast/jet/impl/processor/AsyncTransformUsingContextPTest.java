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

import com.hazelcast.jet.JetException;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.core.test.TestSupport;
import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.pipeline.ContextFactory;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.jet.Traversers.traverseItems;
import static com.hazelcast.jet.core.JetTestSupport.wm;
import static com.hazelcast.jet.core.processor.Processors.flatMapUsingContextAsyncP;
import static com.hazelcast.test.HazelcastTestSupport.spawn;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.concurrent.CompletableFuture.completedFuture;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
public class AsyncTransformUsingContextPTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Parameter
    public boolean ordered;

    @Parameters(name = "ordered={0}")
    public static Collection<Object> parameters() {
        return asList(true, false);
    }

    private ProcessorSupplier getSupplier(DistributedBiFunction<? super String, ? super String,
                    CompletableFuture<Traverser<String>>> mapFn
    ) {
        ContextFactory<String> contextFactory = ContextFactory.withCreateFn(jet -> "foo");
        if (!ordered) {
            contextFactory = contextFactory.unorderedAsyncResponses();
        }
        return flatMapUsingContextAsyncP(contextFactory, DistributedFunction.identity(), mapFn);
    }

    @Test
    public void test_completedFutures() {
        TestSupport
                .verifyProcessor(getSupplier((ctx, item) -> completedFuture(traverseItems(item + "-1", item + "-2"))))
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
                .input(singletonList(wm(10)))
                .expectOutput(singletonList(wm(10)));
    }

    @Test
    public void when_mapFnReturnsNullFuture_then_filteredOut() {
        TestSupport
                .verifyProcessor(getSupplier((ctx, item) -> null))
                .input(asList("a", "b"))
                .expectOutput(emptyList());
    }

    @Test
    public void when_futureReturnsNullTraverser_then_resultFilteredOut() {
        TestSupport
                .verifyProcessor(getSupplier((ctx, item) -> null))
                .input(singletonList(wm(10)))
                .expectOutput(singletonList(wm(10)));
    }

    @Test
    public void when_futureCompletedExceptionally_then_jobFails() {
        exception.expect(JetException.class);
        exception.expectMessage("test exception");
        TestSupport
                .verifyProcessor(getSupplier((ctx, item) -> {
                    CompletableFuture<Traverser<String>> f = new CompletableFuture<>();
                    f.completeExceptionally(new RuntimeException("test exception"));
                    return f;
                }))
                .input(singletonList("a"))
                .expectOutput(emptyList());

    }
}
