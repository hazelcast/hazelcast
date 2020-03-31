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

import com.hazelcast.internal.serialization.impl.SerializationUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.plan.node.PlanNode;
import com.hazelcast.sql.impl.plan.node.PlanNodeVisitor;
import com.hazelcast.sql.impl.type.QueryDataTypeFamily;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Hash join node.
 */
public class HashJoinPlanNode extends AbstractJoinPlanNode {
    /** Left hash keys. */
    private List<Integer> leftHashKeys;

    /** Right hash keys. */
    private List<Integer> rightHashKeys;

    public HashJoinPlanNode() {
        // No-op.
    }

    public HashJoinPlanNode(
        int id,
        PlanNode left,
        PlanNode right,
        Expression<Boolean> condition,
        List<Integer> leftHashKeys,
        List<Integer> rightHashKeys,
        boolean outer,
        boolean semi,
        int rightRowColumnCount
    ) {
        super(id, left, right, condition, outer, semi, rightRowColumnCount);

        // TODO: Fail only if both sides have unresolved types. Otherwise perform coercion during join.
        for (Integer leftHashKey : leftHashKeys) {
            if (left.getSchema().getType(leftHashKey).getTypeFamily() == QueryDataTypeFamily.LATE) {
                throw HazelcastSqlException.error("Column type cannot be resolved: " + leftHashKey);
            }
        }

        for (Integer rightHashKey : rightHashKeys) {
            if (right.getSchema().getType(rightHashKey).getTypeFamily() == QueryDataTypeFamily.LATE) {
                throw HazelcastSqlException.error("Column type cannot be resolved: " + (leftHashKeys.size() + rightHashKey));
            }
        }

        this.leftHashKeys = leftHashKeys;
        this.rightHashKeys = rightHashKeys;
        this.outer = outer;
        this.rightRowColumnCount = rightRowColumnCount;
    }

    public List<Integer> getLeftHashKeys() {
        return leftHashKeys;
    }

    public List<Integer> getRightHashKeys() {
        return rightHashKeys;
    }

    @Override
    public void visit0(PlanNodeVisitor visitor) {
        visitor.onHashJoinNode(this);
    }

    @Override
    public void writeData1(ObjectDataOutput out) throws IOException {
        super.writeData1(out);

        SerializationUtil.writeList(leftHashKeys, out);
        SerializationUtil.writeList(rightHashKeys, out);
    }

    @Override
    public void readData1(ObjectDataInput in) throws IOException {
        super.readData1(in);

        leftHashKeys = SerializationUtil.readList(in);
        rightHashKeys = SerializationUtil.readList(in);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, left, right, condition, leftHashKeys, rightHashKeys, outer, semi, rightRowColumnCount);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        HashJoinPlanNode that = (HashJoinPlanNode) o;

        return id == that.id && left.equals(that.left) && right.equals(that.right) && Objects.equals(condition, that.condition)
            && leftHashKeys.equals(that.leftHashKeys) && rightHashKeys.equals(that.rightHashKeys) && outer == that.outer
            && semi == that.semi && rightRowColumnCount == that.rightRowColumnCount;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{id=" + id + ", condition=" + condition + ", leftHashKeys=" + leftHashKeys
            + ", rightHashKeys=" + rightHashKeys + ", outer=" + outer + ", semi=" + semi
            + ", rightRowColumnCount=" + rightRowColumnCount + ", left=" + left + ", right=" + right + '}';
    }
}
