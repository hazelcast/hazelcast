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

import com.hazelcast.internal.serialization.impl.SerializationUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.physical.PhysicalNode;
import com.hazelcast.sql.impl.physical.PhysicalNodeVisitor;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Hash join node.
 */
public class HashJoinPhysicalNode extends AbstractJoinPhysicalNode {
    /** Left hash keys. */
    private List<Integer> leftHashKeys;

    /** Right hash keys. */
    private List<Integer> rightHashKeys;

    public HashJoinPhysicalNode() {
        // No-op.
    }

    public HashJoinPhysicalNode(
        PhysicalNode left,
        PhysicalNode right,
        Expression<Boolean> condition,
        List<Integer> leftHashKeys,
        List<Integer> rightHashKeys
    ) {
        super(left, right, condition);

        this.leftHashKeys = leftHashKeys;
        this.rightHashKeys = rightHashKeys;
    }

    public List<Integer> getLeftHashKeys() {
        return leftHashKeys;
    }

    public List<Integer> getRightHashKeys() {
        return rightHashKeys;
    }

    @Override
    public void visit(PhysicalNodeVisitor visitor) {
        right.visit(visitor);
        left.visit(visitor);

        visitor.onHashJoinNode(this);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(left);
        out.writeObject(right);
        out.writeObject(condition);
        SerializationUtil.writeList(leftHashKeys, out);
        SerializationUtil.writeList(rightHashKeys, out);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        left = in.readObject();
        right = in.readObject();
        condition = in.readObject();
        leftHashKeys = SerializationUtil.readList(in);
        rightHashKeys = SerializationUtil.readList(in);
    }

    @Override
    public int hashCode() {
        return Objects.hash(left, right, condition);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        HashJoinPhysicalNode that = (HashJoinPhysicalNode) o;

        return left.equals(that.left) && right.equals(that.right) && Objects.equals(condition, that.condition) &&
            leftHashKeys.equals(that.leftHashKeys) && rightHashKeys.equals(that.rightHashKeys);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{condition=" + condition + ", leftHashKeys=" + leftHashKeys
            + ", rightHashKeys=" + rightHashKeys + ", left=" + left + ", right=" + right + '}';
    }
}
