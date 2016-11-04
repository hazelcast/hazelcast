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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static com.hazelcast.jet.impl.util.BasicCompletableFuture.allOf;

class ExecuteJobOperation extends AsyncOperation {

    private DAG dag;

    public ExecuteJobOperation() {
    }

    public ExecuteJobOperation(String engineName, DAG dag) {
        super(engineName);
        this.dag = dag;
    }

    @Override
    public void run() throws Exception {
        JetService service = getService();
        ExecutionContext executionContext = service.getExecutionContext(engineName);
        Map<Member, ExecutionPlan> executionPlanMap = executionContext.buildExecutionPlan(dag);

        invokeForPlan(executionPlanMap, plan -> new InitPlanOperation(engineName, plan))
                .andThen(callback(()
                        -> invokeForPlan(executionPlanMap, plan -> new ExecutePlanOperation(engineName, plan.getId()))
                        .andThen(callback(() -> sendResponse(true)))));
    }

    private ICompletableFuture<List<Object>> invokeForPlan(Map<Member, ExecutionPlan> planMap,
                                                           Function<ExecutionPlan, Operation> func) {
        List<ICompletableFuture<Object>> futures = new ArrayList<>();
        for (Map.Entry<Member, ExecutionPlan> entry : planMap.entrySet()) {
            futures.add(getNodeEngine().getOperationService().createInvocationBuilder(JetService.SERVICE_NAME,
                    func.apply(entry.getValue()), entry.getKey().getAddress()).invoke());
        }
        return allOf(getNodeEngine(), getLogger(), futures);
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

    private ExecutionCallback<List<Object>> callback(final Runnable r) {
        return new ExecutionCallback<List<Object>>() {
            @Override
            public void onResponse(List<Object> response) {
                r.run();
            }

            @Override
            public void onFailure(Throwable t) {
                sendResponse(t);
            }
        };
    }
}
