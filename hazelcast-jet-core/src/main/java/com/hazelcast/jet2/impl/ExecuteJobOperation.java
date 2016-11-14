/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet2.impl;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.Member;
import com.hazelcast.jet2.DAG;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.Operation;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

class ExecuteJobOperation extends AsyncOperation {

    private DAG dag;

    ExecuteJobOperation(String engineName, DAG dag) {
        super(engineName);
        this.dag = dag;
    }

    private ExecuteJobOperation() {
        // for deserialization
    }

    @Override
    protected void doRun() throws Exception {
        JetService service = getService();
        EngineContext engineContext = service.getEngineContext(engineName);
        Map<Member, ExecutionPlan> executionPlanMap = engineContext.newExecutionPlan(dag);
        invokeForPlan(executionPlanMap, plan -> new InitPlanOperation(engineName, plan))
                .thenCompose(x ->
                        invokeForPlan(executionPlanMap, plan -> new ExecutePlanOperation(engineName, plan.getId())))
                .exceptionally(ExecuteJobOperation::peel)
                .thenAccept(this::doSendResponse);
    }

    private CompletableFuture<Object> invokeForPlan(
            Map<Member, ExecutionPlan> planMap, Function<ExecutionPlan, Operation> func
    ) {
        final Stream<ICompletableFuture> futures =
                planMap.entrySet()
                       .stream()
                       .map(e -> getNodeEngine()
                               .getOperationService()
                               .createInvocationBuilder(
                                       JetService.SERVICE_NAME, func.apply(e.getValue()), e.getKey().getAddress())
                               .invoke());
        return allOf(futures.collect(toList()));
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

    private static CompletableFuture<Object> allOf(Collection<ICompletableFuture> futures) {
        final CompletableFuture<Object> compositeFuture = new CompletableFuture<>();
        final AtomicInteger completionLatch = new AtomicInteger(futures.size());
        for (ICompletableFuture future : futures) {
            future.andThen(new ExecutionCallback() {
                @Override
                public void onResponse(Object response) {
                    if (completionLatch.decrementAndGet() == 0) {
                        compositeFuture.complete(true);
                    }
                }

                @Override
                public void onFailure(Throwable t) {
                    compositeFuture.completeExceptionally(t);
                }
            });
        }
        return compositeFuture;
    }

    private static Throwable peel(Throwable e) {
        if (e instanceof CompletionException) {
            return peel(e.getCause());
        }
        return e;
    }
}
