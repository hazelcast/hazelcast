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

package com.hazelcast.sql.impl.physical;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.sql.impl.expression.Expression;

import java.io.IOException;

/**
 * Collocated join node.
 */
public class CollocatedJoinPhysicalNode implements PhysicalNode {
    /** Left input. */
    private PhysicalNode left;

    /** Right input. */
    private PhysicalNode right;

    /** Condition. */
    private Expression<Boolean> condition;

    public CollocatedJoinPhysicalNode() {
        // No-op.
    }

    public CollocatedJoinPhysicalNode(PhysicalNode left, PhysicalNode right, Expression<Boolean> condition) {
        this.left = left;
        this.right = right;
        this.condition = condition;
    }

    public PhysicalNode getLeft() {
        return left;
    }

    public PhysicalNode getRight() {
        return right;
    }

    public Expression<Boolean> getCondition() {
        return condition;
    }

    @Override
    public void visit(PhysicalNodeVisitor visitor) {
        right.visit(visitor);
        left.visit(visitor);

        visitor.onCollocatedJoinNode(this);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(left);
        out.writeObject(right);
        out.writeObject(condition);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        left = in.readObject();
        right = in.readObject();
        condition = in.readObject();
    }
}
