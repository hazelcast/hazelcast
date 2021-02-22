/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.plan.node.io;

import com.hazelcast.internal.serialization.impl.SerializationUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.plan.node.PlanNodeSchema;
import com.hazelcast.sql.impl.plan.node.PlanNodeVisitor;
import com.hazelcast.sql.impl.plan.node.ZeroInputPlanNode;
import com.hazelcast.sql.impl.type.QueryDataType;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Physical node which receives from remote stripes and performs sort-merge.
 */
@SuppressFBWarnings({"EI_EXPOSE_REP", "EI_EXPOSE_REP2"})
public class ReceiveSortMergePlanNode extends ZeroInputPlanNode implements EdgeAwarePlanNode, IdentifiedDataSerializable {
    /**
     * Edge iD.
     */
    private int edgeId;

    /**
     * Field types.
     */
    private List<QueryDataType> fieldTypes;

    /**
     * Indexes of columns to be used for sorting.
     */
    private int[] columnIndexes;

    /**
     * Sort directions.
     */
    private boolean[] ascs;

    /**
     * Limit expression.
     */
    private Expression fetch;

    /**
     * Offset expression.
     */
    private Expression offset;


    public ReceiveSortMergePlanNode() {
        // No-op.
    }

    public ReceiveSortMergePlanNode(
        int id,
        int edgeId,
        List<QueryDataType> fieldTypes,
        int[] columnIndexes,
        boolean[] ascs,
        Expression fetch,
        Expression offset
    ) {
        super(id);

        this.edgeId = edgeId;
        this.fieldTypes = fieldTypes;
        this.columnIndexes = columnIndexes;
        this.ascs = ascs;
        this.fetch = fetch;
        this.offset = offset;
    }

    public int[] getColumnIndexes() {
        return columnIndexes;
    }

    public boolean[] getAscs() {
        return ascs;
    }

    public Expression getFetch() {
        return fetch;
    }

    public Expression getOffset() {
        return offset;
    }

    @Override
    public int getEdgeId() {
        return edgeId;
    }

    @Override
    public boolean isSender() {
        return false;
    }

    @Override
    public void visit(PlanNodeVisitor visitor) {
        visitor.onReceiveSortMergeNode(this);
    }

    @Override
    public PlanNodeSchema getSchema0() {
        return new PlanNodeSchema(fieldTypes);
    }

    @Override
    public void writeData0(ObjectDataOutput out) throws IOException {
        out.writeInt(edgeId);
        SerializationUtil.writeList(fieldTypes, out);
        out.writeInt(columnIndexes.length);
        for (int i = 0; i < columnIndexes.length; ++i) {
            out.writeInt(columnIndexes[i]);
        }
        out.writeInt(ascs.length);
        for (int i = 0; i < ascs.length; ++i) {
            out.writeBoolean(ascs[i]);
        }
        out.writeObject(fetch);
        out.writeObject(offset);
    }

    @Override
    public void readData0(ObjectDataInput in) throws IOException {
        edgeId = in.readInt();
        fieldTypes = SerializationUtil.readList(in);
        int columnIndexesLength = in.readInt();
        columnIndexes = new int[columnIndexesLength];
        for (int i = 0; i < columnIndexesLength; ++i) {
            columnIndexes[i] = in.readInt();
        }
        int ascsLength = in.readInt();
        ascs = new boolean[ascsLength];
        for (int i = 0; i < ascsLength; ++i) {
            ascs[i] = in.readBoolean();
        }
        fetch = in.readObject();
        offset = in.readObject();
    }

    @Override
    public int getFactoryId() {
        return SqlDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SqlDataSerializerHook.NODE_RECEIVE_MERGE_SORT;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, edgeId, columnIndexes, fieldTypes, ascs, fetch, offset);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ReceiveSortMergePlanNode that = (ReceiveSortMergePlanNode) o;

        return id == that.id && edgeId == that.edgeId
            && Arrays.equals(columnIndexes, that.columnIndexes)
            && Arrays.equals(ascs, that.ascs)
            && fieldTypes.equals(that.fieldTypes)
            && Objects.equals(fetch, that.fetch)
            && Objects.equals(offset, that.offset);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{id=" + id + ", edgeId=" + edgeId + ", columnIndexes="
            + Arrays.toString(columnIndexes) + ", ascs=" + Arrays.toString(ascs) + ", fieldTypes=" + fieldTypes
            + ", fetch=" + fetch + ", offset=" + offset + '}';
    }
}
