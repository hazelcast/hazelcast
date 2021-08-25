/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl;

import com.hazelcast.internal.util.Preconditions;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.sql.impl.expression.Expression;
import org.apache.calcite.rel.core.JoinRelType;

import java.io.IOException;
import java.util.Arrays;

/**
 * An analyzed join condition.
 * <p>
 * Contains:<ul>
 * <li>{@code joinType}: type of join
 * <li>{@code leftEquiJoinIndices}: indices of the fields from the left side of
 * a join which are equi-join keys
 * <li>{@code rightEquiJoinIndices}: indices of the fields from the right side
 * of a join which are equi-join keys
 * <li>{@code nonEquiCondition}: remaining join filters that are not equi-joins
 * <li>{@code condition}: all join filters
 * </ul>
 */
public class JetJoinInfo implements DataSerializable {

    private JoinRelType joinType;

    private int[] leftEquiJoinIndices;
    private int[] rightEquiJoinIndices;
    private Expression<Boolean> nonEquiCondition;

    private Expression<Boolean> condition;

    @SuppressWarnings("unused")
    private JetJoinInfo() {
    }

    public JetJoinInfo(
            JoinRelType joinType,
            int[] leftEquiJoinIndices,
            int[] rightEquiJoinIndices,
            Expression<Boolean> nonEquiCondition,
            Expression<Boolean> condition
    ) {
        Preconditions.checkTrue(leftEquiJoinIndices.length == rightEquiJoinIndices.length, "indices length mismatch");

        this.joinType = joinType;

        this.leftEquiJoinIndices = leftEquiJoinIndices;
        this.rightEquiJoinIndices = rightEquiJoinIndices;
        this.nonEquiCondition = nonEquiCondition;

        this.condition = condition;
    }

    /**
     * Returns true if the join type is an INNER join.
     */
    public boolean isInner() {
        return joinType == JoinRelType.INNER;
    }

    /**
     * Returns true if the join type is a LEFT OUTER join.
     */
    public boolean isLeftOuter() {
        return joinType == JoinRelType.LEFT;
    }

    /**
     * The indices of the fields from the left side of a join which are
     * equi-join keys.
     */
    public int[] leftEquiJoinIndices() {
        return leftEquiJoinIndices;
    }

    /**
     * The indices of the fields from the right side of a join which are
     * equi-join keys. The indices refer to fields of right table before
     * applying projection or before joining.
     */
    public int[] rightEquiJoinIndices() {
        return rightEquiJoinIndices;
    }

    /**
     * Remaining join filters that are not equi-joins. Column references in
     * this condition apply to the joined row.
     */
    public Expression<Boolean> nonEquiCondition() {
        return nonEquiCondition;
    }

    /**
     * All join filters. Column references in this condition apply to the
     * joined row.
     */
    public Expression<Boolean> condition() {
        return condition;
    }

    public boolean isEquiJoin() {
        return rightEquiJoinIndices.length > 0;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(joinType.name());
        out.writeObject(leftEquiJoinIndices);
        out.writeObject(rightEquiJoinIndices);
        out.writeObject(nonEquiCondition);
        out.writeObject(condition);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        joinType = JoinRelType.valueOf(in.readString());
        leftEquiJoinIndices = in.readObject();
        rightEquiJoinIndices = in.readObject();
        nonEquiCondition = in.readObject();
        condition = in.readObject();
    }

    @Override
    public String toString() {
        return "JetJoinInfo{" +
                "joinType=" + joinType.name() +
                ", leftEquiJoinIndices=" + Arrays.toString(leftEquiJoinIndices) +
                ", rightEquiJoinIndices=" + Arrays.toString(rightEquiJoinIndices) +
                ", nonEquiCondition=" + nonEquiCondition +
                ", condition=" + condition +
                '}';
    }
}
