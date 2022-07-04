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

package com.hazelcast.jet.sql.impl.processors;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.impl.memory.AccumulationLimitExceededException;
import com.hazelcast.jet.sql.impl.ExpressionUtil;
import com.hazelcast.jet.sql.impl.JetJoinInfo;
import com.hazelcast.sql.impl.row.JetSqlRow;
import com.hazelcast.jet.sql.impl.ObjectArrayKey;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class SqlHashJoinP extends AbstractProcessor {

    private final JetJoinInfo joinInfo;
    private final int rightInputColumnCount;

    private ExpressionEvalContext evalContext;
    private Multimap<ObjectArrayKey, JetSqlRow> hashMap;
    private FlatMapper<JetSqlRow, JetSqlRow> flatMapper;
    private long maxItemsInHashTable;

    public SqlHashJoinP(JetJoinInfo joinInfo, int rightInputColumnCount) {
        this.joinInfo = joinInfo;
        this.rightInputColumnCount = rightInputColumnCount;
    }

    @Override
    public void init(@Nonnull Context context) throws Exception {
        this.evalContext = ExpressionEvalContext.from(context);
        this.hashMap = LinkedListMultimap.create();
        this.flatMapper = flatMapper(this::join);
        this.maxItemsInHashTable = context.maxProcessorAccumulatedRecords();
    }

    private Traverser<JetSqlRow> join(JetSqlRow leftRow) {
        ObjectArrayKey joinKeys = ObjectArrayKey.project(leftRow, joinInfo.leftEquiJoinIndices());
        Collection<JetSqlRow> matchedRows = hashMap.get(joinKeys);
        List<JetSqlRow> output = matchedRows.stream()
                .map(right -> ExpressionUtil.join(
                        leftRow,
                        right,
                        joinInfo.nonEquiCondition(),
                        evalContext)
                )
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        if (joinInfo.isLeftOuter() && output.isEmpty()) {
            return Traversers.singleton(leftRow.extendedRow(rightInputColumnCount));
        }
        return Traversers.traverseIterable(output);
    }

    @Override
    protected boolean tryProcess0(@Nonnull Object item) {
        return flatMapper.tryProcess((JetSqlRow) item);
    }

    @Override
    protected boolean tryProcess1(@Nonnull Object item) {
        if (hashMap.size() == maxItemsInHashTable) {
            throw new AccumulationLimitExceededException();
        }
        JetSqlRow rightRow = (JetSqlRow) item;
        ObjectArrayKey joinKeys = ObjectArrayKey.project(rightRow, joinInfo.rightEquiJoinIndices());
        // if there's a null in the key, then `null = null` is UNKNOWN in SQL, ignore such keys
        if (joinKeys.containsNull()) {
            return true;
        }
        hashMap.put(joinKeys, rightRow);
        return true;
    }

    @Override
    public boolean tryProcessWatermark(@Nonnull Watermark watermark) {
        return true;
    }

    public static HashJoinProcessorSupplier supplier(JetJoinInfo joinInfo, int rightInputColumnCount) {
        return new HashJoinProcessorSupplier(joinInfo, rightInputColumnCount);
    }

    private static final class HashJoinProcessorSupplier implements ProcessorSupplier, DataSerializable {
        private JetJoinInfo joinInfo;
        private int rightInputColumnCount;

        @SuppressWarnings("unused") // for deserialization
        private HashJoinProcessorSupplier() {
        }

        private HashJoinProcessorSupplier(JetJoinInfo joinInfo, int rightInputColumnCount) {
            this.joinInfo = joinInfo;
            this.rightInputColumnCount = rightInputColumnCount;
        }

        @Nonnull
        @Override
        public Collection<? extends Processor> get(int count) {
            List<SqlHashJoinP> processors = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                processors.add(new SqlHashJoinP(joinInfo, rightInputColumnCount));
            }
            return processors;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeObject(joinInfo);
            out.writeInt(rightInputColumnCount);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            joinInfo = in.readObject();
            rightInputColumnCount = in.readInt();
        }
    }
}
