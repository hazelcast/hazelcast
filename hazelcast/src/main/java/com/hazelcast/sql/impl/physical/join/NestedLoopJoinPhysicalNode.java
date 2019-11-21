/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.physical.join;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.physical.PhysicalNode;
import com.hazelcast.sql.impl.physical.PhysicalNodeVisitor;

import java.io.IOException;
import java.util.Objects;

/**
 * Nested loop join node.
 */
public class NestedLoopJoinPhysicalNode extends AbstractJoinPhysicalNode {
    /** Outer join flag. */
    private boolean outer;

    /** Number of columns in the right row. */
    private int rightRowColumnCount;

    public NestedLoopJoinPhysicalNode() {
        // No-op.
    }

    public NestedLoopJoinPhysicalNode(
        PhysicalNode left,
        PhysicalNode right,
        Expression<Boolean> condition,
        boolean outer,
        int rightRowColumnCount
    ) {
        super(left, right, condition);

        this.outer = outer;
        this.rightRowColumnCount = rightRowColumnCount;
    }

    public boolean isOuter() {
        return outer;
    }

    public int getRightRowColumnCount() {
        return rightRowColumnCount;
    }

    @Override
    public void visit(PhysicalNodeVisitor visitor) {
        right.visit(visitor);
        left.visit(visitor);

        visitor.onNestedLoopJoinNode(this);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(left);
        out.writeObject(right);
        out.writeObject(condition);
        out.writeBoolean(outer);
        out.writeInt(rightRowColumnCount);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        left = in.readObject();
        right = in.readObject();
        condition = in.readObject();
        outer = in.readBoolean();
        rightRowColumnCount = in.readInt();
    }

    @Override
    public int hashCode() {
        return Objects.hash(left, right, condition, outer, rightRowColumnCount);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        NestedLoopJoinPhysicalNode that = (NestedLoopJoinPhysicalNode) o;

        return left.equals(that.left) && right.equals(that.right) && Objects.equals(condition, that.condition)
            && outer == that.outer && rightRowColumnCount == that.rightRowColumnCount;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{condition=" + condition + ", outer=" + outer
            + ", rightRowColumnCount=" + rightRowColumnCount + ", left=" + left + ", right=" + right + '}';
    }
}
