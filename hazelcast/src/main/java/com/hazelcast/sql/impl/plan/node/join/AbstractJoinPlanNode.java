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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.plan.node.BiInputPlanNode;
import com.hazelcast.sql.impl.plan.node.PlanNode;
import com.hazelcast.sql.impl.plan.node.PlanNodeSchema;

import java.io.IOException;

/**
 * Abstract join node.
 */
public abstract class AbstractJoinPlanNode extends BiInputPlanNode {
    /** Join condition. */
    protected Expression<Boolean> condition;

    /** Outer join flag. */
    protected boolean outer;

    /** Semi join flag. */
    protected boolean semi;

    /** Number of columns in the right row. */
    protected int rightRowColumnCount;

    protected AbstractJoinPlanNode() {
        // No-op.
    }

    protected AbstractJoinPlanNode(
        int id,
        PlanNode left,
        PlanNode right,
        Expression<Boolean> condition,
        boolean outer,
        boolean semi,
        int rightRowColumnCount
    ) {
        super(id, left, right);

        this.condition = condition;
        this.outer = outer;
        this.semi = semi;
        this.rightRowColumnCount = rightRowColumnCount;
    }

    public Expression<Boolean> getCondition() {
        return condition;
    }

    public boolean isOuter() {
        return outer;
    }

    public boolean isSemi() {
        return semi;
    }

    public int getRightRowColumnCount() {
        return rightRowColumnCount;
    }

    @Override
    public PlanNodeSchema getSchema0() {
        return PlanNodeSchema.combine(left.getSchema(), right.getSchema());
    }

    @Override
    public void writeData1(ObjectDataOutput out) throws IOException {
        out.writeObject(condition);
        out.writeBoolean(outer);
        out.writeInt(rightRowColumnCount);
    }

    @Override
    public void readData1(ObjectDataInput in) throws IOException {
        condition = in.readObject();
        outer = in.readBoolean();
        rightRowColumnCount = in.readInt();
    }
}
