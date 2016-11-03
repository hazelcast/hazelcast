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

import com.hazelcast.core.Member;
import com.hazelcast.jet.impl.util.JetUtil;
import com.hazelcast.jet2.DAG;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.Operation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.function.Function;

class ExecuteJobOperation extends Operation {

    private String engineName;
    private DAG dag;

    public ExecuteJobOperation() {
    }

    public ExecuteJobOperation(String engineName, DAG dag) {
        this.engineName = engineName;
        this.dag = dag;
    }

    @Override
    public void run() throws Exception {
        JetService service = getService();
        ExecutionContext executionContext = service.getExecutionContext(engineName);
        Map<Member, ExecutionPlan> executionPlanMap = executionContext.buildExecutionPlan(dag);

        invokeForPlan(executionPlanMap, plan -> new InitPlanOperation(engineName, plan));
        invokeForPlan(executionPlanMap, plan -> new ExecutePlanOperation(engineName, plan.getId()));
    }

    private void invokeForPlan(Map<Member, ExecutionPlan> planMap, Function<ExecutionPlan, Operation> func) {
        List<Future> futures = new ArrayList<>();
        for (Map.Entry<Member, ExecutionPlan> entry : planMap.entrySet()) {
            futures.add(getNodeEngine().getOperationService().createInvocationBuilder(JetService.SERVICE_NAME,
                    func.apply(entry.getValue()), entry.getKey().getAddress()).invoke());
        }
        for (Future future : futures) {
            JetUtil.uncheckedGet(future);
        }
    }


    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);

        out.writeUTF(engineName);
        out.writeObject(dag);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);

        engineName = in.readUTF();
        dag = in.readObject();
    }
}
