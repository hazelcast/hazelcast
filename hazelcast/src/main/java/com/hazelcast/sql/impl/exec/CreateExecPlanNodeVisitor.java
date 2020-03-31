/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.exec;

import com.hazelcast.sql.impl.exec.root.RootExec;
import com.hazelcast.sql.impl.operation.QueryExecuteOperation;
import com.hazelcast.sql.impl.plan.node.PlanNode;
import com.hazelcast.sql.impl.plan.node.PlanNodeVisitor;
import com.hazelcast.sql.impl.plan.node.RootPlanNode;

import java.util.ArrayList;

 /**
 * Visitor which builds an executor for every observed physical node.
 */
public class CreateExecPlanNodeVisitor implements PlanNodeVisitor {
    /** Operation. */
    private final QueryExecuteOperation operation;

    /** Stack of elements to be merged. */
    private final ArrayList<Exec> stack = new ArrayList<>(1);

    /** Result. */
    private Exec exec;

    public CreateExecPlanNodeVisitor(
        QueryExecuteOperation operation
    ) {
        this.operation = operation;
    }

    @Override
    public void onRootNode(RootPlanNode node) {
        assert stack.size() == 1;

        exec = new RootExec(
            node.getId(),
            pop(),
            operation.getRootConsumer(),
            operation.getRootBatchSize()
        );
    }

    @Override
    public void onOtherNode(PlanNode node) {
        if (node instanceof CreateExecPlanNodeVisitorCallback) {
            ((CreateExecPlanNodeVisitorCallback) node).onVisit(this);
        } else {
            throw new UnsupportedOperationException("Unsupported node: " + node);
        }
    }

    public Exec getExec() {
        return exec;
    }

    public Exec pop() {
        return stack.remove(stack.size() - 1);
    }

    public void push(Exec exec) {
        stack.add(exec);
    }
}
