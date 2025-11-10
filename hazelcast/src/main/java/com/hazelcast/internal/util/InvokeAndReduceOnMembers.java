/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.util;

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationService;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.hazelcast.internal.util.ConcurrencyUtil.CALLER_RUNS;

public class InvokeAndReduceOnMembers<T, R> {
    private static final int TRY_COUNT = 10;
    private static final int TRY_PAUSE_MILLIS = 300;

    private final Collection<Member> members;
    private final OperationService operationService;
    private final Supplier<Operation> operationSupplier;
    private final Function<Map<Member, T>, R> reduceFunction;
    private final BiFunction<T, Throwable, T> mapFunction;

    public InvokeAndReduceOnMembers(
            Collection<Member> members,
            Supplier<Operation> operationSupplier,
            OperationService operationService,
            BiFunction<T, Throwable, T> mapFunction,
            Function<Map<Member, T>, R> reduceFunction
    ) {
        this.members = members;
        this.operationService = operationService;
        this.operationSupplier = operationSupplier;
        this.reduceFunction = reduceFunction;
        this.mapFunction = mapFunction;
    }

    CompletableFuture<R> invokeAsync() {
        if (members.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        var collector = new ResultCollector<>(members, reduceFunction, mapFunction);
        for (final var target : members) {
            final Address address = target.getAddress();
            var operation = operationSupplier.get();
            operationService.createInvocationBuilder(operation.getServiceName(), operation, address)
                    .setTryCount(TRY_COUNT)
                    .setTryPauseMillis(TRY_PAUSE_MILLIS)
                    .setResultDeserialized(false)
                    .invoke()
                    .whenCompleteAsync(
                            (r, t) -> collector.accept(target, (T) r, t),
                            CALLER_RUNS
                    );
        }
        return collector.future;
    }

    private static class ResultCollector<T, R> {
        private final CompletableFuture<R> future = new CompletableFuture<>();
        private final AtomicInteger pendingResponses = new AtomicInteger();
        private final Map<Member, T> results;
        private final Function<Map<Member, T>, R> reduce;
        private final BiFunction<T, Throwable, T> map;

        private ResultCollector(
                Collection<Member> targets,
                Function<Map<Member, T>, R> reduce,
                BiFunction<T, Throwable, T> map) {
            this.pendingResponses.set(targets.size());
            this.results = Collections.synchronizedMap(new HashMap<>(targets.size()));
            this.reduce = reduce;
            this.map = map;
        }

        public void accept(Member target, T result, Throwable throwable) {
            T mappedResult;
            try {
                mappedResult = map.apply(result, throwable);
            } catch (Throwable t) {
                // The entire process has failed; no need to control pendingResponses.
                future.completeExceptionally(t);
                return;
            }

            if (results.putIfAbsent(target, mappedResult) != null) {
                future.completeExceptionally(new IllegalArgumentException("Duplicate response from -> " + target));
                return;
            }

            if (pendingResponses.decrementAndGet() > 0) {
                return;
            }

            try {
                future.complete(reduce.apply(results));
            } catch (Throwable t) {
                future.completeExceptionally(t);
            }
        }
    }
}
