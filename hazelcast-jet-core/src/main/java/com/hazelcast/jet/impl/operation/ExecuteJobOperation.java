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

package com.hazelcast.jet.impl.operation;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.Member;
import com.hazelcast.jet.DAG;
import com.hazelcast.jet.impl.JetService;
import com.hazelcast.jet.impl.execution.init.ExecutionPlan;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.Operation;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.hazelcast.jet.impl.util.ExceptionUtil.peel;
import static com.hazelcast.spi.InvocationBuilder.DEFAULT_TRY_COUNT;
import static java.util.stream.Collectors.toList;

public class ExecuteJobOperation extends AsyncExecutionOperation {

    private DAG dag;
    private volatile CompletableFuture<Object> executionInvocationFuture;
    private Throwable cachedExceptionResult;

    public ExecuteJobOperation(long executionId, DAG dag) {
        super(executionId);
        this.dag = dag;
    }

    private ExecuteJobOperation() {
        // for deserialization
    }

    @Override
    protected void doRun() throws Exception {
        long start = System.currentTimeMillis();
        getLogger().info("Start executing job " + executionId + ": " + dag);
        JetService service = getService();
        getLogger().fine("Building execution plan for job " + executionId + '.');
        Map<Member, ExecutionPlan> executionPlanMap = service.createExecutionPlans(dag);
        getLogger().fine("Built execution plan for job " + executionId + '.');

        // Future that is signalled on a failure during Init
        CompletableFuture<Object> init = invokeOnCluster(executionPlanMap,
                plan -> new InitOperation(executionId, plan), DEFAULT_TRY_COUNT);
        CompletableFuture<Throwable> initFailed = onException(init);

        // Future that is completed on the real completion of all Execute operations
        CompletableFuture<Void> executionDone = new CompletableFuture<>();
        CompletableFuture<Throwable> execution =
                // ExecuteOperation should only be run if InitOperation succeeded
                init.thenCompose(x -> executionInvocationFuture = invokeOnCluster(executionPlanMap,
                        plan -> new ExecuteOperation(executionId), executionDone, true, DEFAULT_TRY_COUNT))
                    .handle((v, e) -> e != null ? peel(e) : null);

        // CompleteOperation is fired regardless of success of previous operations
        // It must be fired _after_ all Execute operation are done, or in case of failure during Init phase,
        // after all Init operations are finished.
        CompletableFuture<Throwable> completion =
                CompletableFuture.anyOf(initFailed, executionDone)
                                 .thenCombine(execution, (r, e) -> e)
                                 .thenCompose(e -> invokeOnCluster(executionPlanMap, plan ->
                                         new CompleteOperation(executionId, e), 3))
                                 .handle((v, e) -> e != null ? peel(e) : null);

        // Exception from ExecuteOperation should have precedence
        execution
                .thenCombine(completion, (e1, e2) -> e1 == null ? e2 : e1)
                .exceptionally(Function.identity())
                .thenAccept(e -> {
                    long elapsed = System.currentTimeMillis() - start;
                    getLogger().info("Execution of job " + executionId + " completed in " + elapsed + "ms.");
                    doSendResponse(e);
                });
        if (cachedExceptionResult != null) {
            executionInvocationFuture.completeExceptionally(cachedExceptionResult);
        }
    }

    private <E> CompletableFuture<Object> invokeOnCluster(Map<Member, E> memberMap, Function<E, Operation> func,
                                                          int tryCount) {
        return invokeOnCluster(memberMap, func, new CompletableFuture<>(), false, tryCount);
    }

    private <E> CompletableFuture<Object> invokeOnCluster(Map<Member, E> memberMap, Function<E, Operation> func,
                                                          CompletableFuture<Void> doneFuture, boolean propagateError,
                                                          int tryCount) {
        AtomicInteger doneLatch = new AtomicInteger(memberMap.size());
        final Stream<ICompletableFuture> futures =
                memberMap.entrySet().stream().map(e -> getNodeEngine()
                        .getOperationService()
                        .createInvocationBuilder(JetService.SERVICE_NAME,
                                func.apply(e.getValue()), e.getKey().getAddress())
                        .setDoneCallback(() -> {
                            if (doneLatch.decrementAndGet() == 0) {
                                doneFuture.complete(null);
                            }
                        })
                        .setTryCount(tryCount)
                        .invoke());
        return allOf(futures.collect(toList()), propagateError);
    }

    @Override
    public void cancel() {
        if (executionInvocationFuture != null) {
            getLogger().info("Cancelling job " + executionId);
            executionInvocationFuture.cancel(true);
        }
    }

    @Override
    public void completeExceptionally(Throwable throwable) {
        if (executionInvocationFuture == null) {
            this.cachedExceptionResult = throwable;
        } else {
            executionInvocationFuture.completeExceptionally(throwable);
        }
    }


    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(dag);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        dag = in.readObject();
    }

    // Combines several invocation futures into one. Completes only when all of the sub-futures have completed or
    // when the composite future is cancelled from outside
    private static CompletableFuture<Object> allOf(final Collection<ICompletableFuture> futures, boolean propagateError) {
        final CompletableFuture<Object> compositeFuture = new CompletableFuture<>();
        compositeFuture.whenComplete((r, e) -> {
            if (e instanceof CancellationException) {
                futures.forEach(f -> f.cancel(true));
            }
        });

        final AtomicInteger completionLatch = new AtomicInteger(futures.size());
        final AtomicReference<Throwable> firstError = new AtomicReference<>();
        futures.forEach(f -> f.andThen(new Callback((r) -> {
            if (completionLatch.decrementAndGet() == 0) {
                Throwable error = firstError.get();
                if (error == null) {
                    compositeFuture.complete(null);
                } else {
                    compositeFuture.completeExceptionally(error);
                }
            }
        }, e -> {
            firstError.compareAndSet(null, e);
            // cancel all other futures immediately when an error is detected
            if (propagateError) {
                futures.forEach(sub -> sub.cancel(true));
            }
            if (completionLatch.decrementAndGet() == 0) {
                compositeFuture.completeExceptionally(firstError.get());
            }
        })));
        return compositeFuture;
    }

    /**
     * Returns a new {@link CompletableFuture}, that will complete normally, when {@code stage} completes exceptionally,
     * with the Throwable as a value.
     * <p>However, if {@code stage} completes normally, the returned stage will never complete.
     */
    private static <T> CompletableFuture<Throwable> onException(CompletableFuture<T> stage) {
        CompletableFuture<Throwable> f = new CompletableFuture<>();
        stage.whenComplete((r, e) -> {
            if (e != null) {
                f.complete(e);
            }
        });
        return f;
    }

    // lambda-friendly wrapper for ExecutionCallback
    private static final class Callback implements ExecutionCallback {

        private final Consumer<Object> onResponse;
        private final Consumer<Throwable> onError;

        private Callback(Consumer<Object> onResponse, Consumer<Throwable> onError) {
            this.onResponse = onResponse;
            this.onError = onError;
        }

        @Override
        public void onResponse(Object response) {
            onResponse.accept(response);
        }

        @Override
        public void onFailure(Throwable t) {
            onError.accept(t);
        }
    }
}
