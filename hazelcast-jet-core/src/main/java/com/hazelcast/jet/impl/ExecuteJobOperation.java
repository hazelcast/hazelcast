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

package com.hazelcast.jet.impl;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.Member;
import com.hazelcast.jet.DAG;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.Operation;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

public class ExecuteJobOperation extends AsyncOperation {

    private DAG dag;
    private long executionId;
    private volatile CompletableFuture<Object> executePlanFuture;

    public ExecuteJobOperation(String engineName, long executionId, DAG dag) {
        super(engineName);
        this.executionId = executionId;
        this.dag = dag;
    }

    private ExecuteJobOperation() {
        // for deserialization
    }

    @Override
    protected void doRun() throws Exception {
        JetService service = getService();
        EngineContext engineContext = service.getEngineContext(engineName);
        Map<Member, ExecutionPlan> executionPlanMap = engineContext.newExecutionPlan(executionId, dag);
        invokeForPlan(executionPlanMap, plan -> new InitPlanOperation(engineName, plan))
                .thenCompose(x -> executePlanFuture =
                        invokeForPlan(executionPlanMap, plan -> new ExecutePlanOperation(engineName, plan.getPlanId())))
                .exceptionally(Util::peel)
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
    void cancel() {
        if (executePlanFuture != null) {
            executePlanFuture.cancel(true);
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(executionId);
        out.writeObject(dag);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        executionId = in.readLong();
        dag = in.readObject();
    }

    private static CompletableFuture<Object> allOf(final Collection<ICompletableFuture> futures) {
        final CompletableFuture<Object> compositeFuture = new CompletableFuture<>();
        compositeFuture.whenComplete((r, e) -> {
            if (e != null) {
                futures.forEach(f -> f.cancel(true));
            }
        });
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
}
