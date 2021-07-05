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

package com.hazelcast.jet.sql.impl.connector.map;

import com.hazelcast.internal.serialization.impl.SerializationUtil;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.ResettableSingletonTraverser;
import com.hazelcast.jet.impl.processor.TransformP;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.sql.impl.expression.ColumnExpression;
import com.hazelcast.sql.impl.expression.Expression;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public final class RowReducerSupplier implements ProcessorSupplier, DataSerializable {
    private List<Integer> usedColumns;

    @SuppressWarnings("unused")
    private RowReducerSupplier() {
        // no-op
    }

    private RowReducerSupplier(List<Integer> usedColumns) {
        this.usedColumns = usedColumns;
    }

    @Nonnull
    @Override
    public Collection<? extends Processor> get(int count) {
        List<Processor> processors = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            ResettableSingletonTraverser<Object[]> traverser = new ResettableSingletonTraverser<>();
            Processor processor = new TransformP<Object[], Object[]>(entry -> {
                traverser.accept(reduce(entry));
                return traverser;
            });
            processors.add(processor);
        }
        return processors;
    }

    public Object[] reduce(Object[] entry) {
        Object[] row = new Object[usedColumns.size()];
        int idx = 0;
        for (Integer i : usedColumns) {
            assert i < entry.length;
            row[idx++] = entry[i];
        }
        return row;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        SerializationUtil.writeList(usedColumns, out);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        usedColumns = SerializationUtil.readList(in);
    }

    public static ProcessorSupplier rowReducer(List<Expression<?>> projection) {
        return new RowReducerSupplier(extractIndices(projection));
    }

    @SuppressWarnings("rawtypes")
    private static List<Integer> extractIndices(List<Expression<?>> projection) {
        List<Integer> usedColumns = new ArrayList<>();

        for (Expression<?> expression : projection) {
            assert expression instanceof ColumnExpression;
            ColumnExpression expr = (ColumnExpression) expression;
            usedColumns.add(expr.getIndex());
        }
        return usedColumns;
    }
}
