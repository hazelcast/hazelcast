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

package com.hazelcast.sql.impl.plan.node.join;

import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.plan.node.PlanNode;
import com.hazelcast.sql.impl.plan.node.PlanNodeVisitor;

import java.util.Objects;

/**
 * Nested loop join node.
 */
public class NestedLoopJoinPlanNode extends AbstractJoinPlanNode {
    public NestedLoopJoinPlanNode() {
        // No-op.
    }

    public NestedLoopJoinPlanNode(
        int id,
        PlanNode left,
        PlanNode right,
        Expression<Boolean> condition,
        boolean outer,
        boolean semi,
        int rightRowColumnCount
    ) {
        super(id, left, right, condition, outer, semi, rightRowColumnCount);
    }

    @Override
    public void visit0(PlanNodeVisitor visitor) {
        visitor.onNestedLoopJoinNode(this);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, left, right, condition, outer, semi, rightRowColumnCount);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        NestedLoopJoinPlanNode that = (NestedLoopJoinPlanNode) o;

        return id == that.id && left.equals(that.left) && right.equals(that.right) && Objects.equals(condition, that.condition)
            && outer == that.outer && semi == that.semi && rightRowColumnCount == that.rightRowColumnCount;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{id=" + id + ", condition=" + condition + ", outer=" + outer + ", semi=" + semi
            + ", rightRowColumnCount=" + rightRowColumnCount + ", left=" + left + ", right=" + right + '}';
    }
}
