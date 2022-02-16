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

import java.nio.ByteOrder;
import java.util.Collections;
import java.util.List;

import javax.annotation.Nonnull;

import com.hazelcast.core.ManagedContext;
import com.hazelcast.internal.nio.BufferObjectDataInput;
import com.hazelcast.internal.nio.BufferObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.DataType;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.InternalGenericRecord;
import com.hazelcast.internal.serialization.impl.compact.Schema;
import com.hazelcast.internal.serialization.impl.portable.PortableContext;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.partition.PartitioningStrategy;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.row.Row;

final class ExpressionConstEvaluator {

    private ExpressionConstEvaluator() {
    }

    /**
     * Evaluate expression. If it touches the row or the context so the evaluation stops
     * and {@code null} is returned. If succeeds, constant expression with the ready value of the same type is
     * returned.
     */
    public static ConstantExpression<?> evaluate(
        @Nonnull Expression<?> expression
    ) {
        try {
            Object val = expression.eval(constEvalRow(), constEvalContext());
            return ConstantExpression.create(val, expression.getType());
        } catch (ConstEvalException e) {
            return null;
        }
    }

    private static Row constEvalRow() {
        return ConstEvalRow.INSTANCE;
    }

    private static ExpressionEvalContext constEvalContext() {
        return ConstEvalContext.INSTANCE;
    }

    private static final class ConstEvalRow implements Row {

        private static final ConstEvalRow INSTANCE = new ConstEvalRow();

        @Override
        public <T> T get(int index) {
            throw ConstEvalException.INSTANCE;
        }

        @Override
        public int getColumnCount() {
            throw ConstEvalException.INSTANCE;
        }

    }

    private static final class ConstEvalContext extends ExpressionEvalContext {

        private static final ConstEvalContext INSTANCE = new ConstEvalContext();

        private ConstEvalContext() {
            super(Collections.emptyList(), new ConstEvalSerializationService());
        }

        @Override
        public Object getArgument(int index) {
            throw ConstEvalException.INSTANCE;
        }

        @Override
        public List<Object> getArguments() {
            throw ConstEvalException.INSTANCE;
        }

        @Override
        public InternalSerializationService getSerializationService() {
            throw ConstEvalException.INSTANCE;
        }

    }

    private static final class ConstEvalException extends RuntimeException {
        private static final ConstEvalException INSTANCE = new ConstEvalException();

        private ConstEvalException() {
            super("", null, false, false);
        }
    }

    @SuppressWarnings("rawtypes")
    private static class ConstEvalSerializationService implements InternalSerializationService {

        @Override
        public void dispose() {
            throw ConstEvalException.INSTANCE;
        }

        @Override
        public byte[] toBytes(Object obj) {
            throw ConstEvalException.INSTANCE;
        }

        @Override
        public byte[] toBytes(Object obj, int leftPadding, boolean insertPartitionHash) {
            throw ConstEvalException.INSTANCE;
        }

        @Override
        public <B extends Data> B toData(Object obj, DataType type) {
            throw ConstEvalException.INSTANCE;
        }

        @Override
        public <B extends Data> B toData(Object obj, DataType type, PartitioningStrategy strategy) {
            throw ConstEvalException.INSTANCE;
        }

        @Override
        public <B extends Data> B convertData(Data data, DataType type) {
            throw ConstEvalException.INSTANCE;
        }

        @Override
        public void writeObject(ObjectDataOutput out, Object obj) {
            throw ConstEvalException.INSTANCE;
        }

        @Override
        public <T> T readObject(ObjectDataInput in) {
            throw ConstEvalException.INSTANCE;
        }

        @Override
        public <T> T readObject(ObjectDataInput in, Class aClass) {
            throw ConstEvalException.INSTANCE;
        }

        @Override
        public void disposeData(Data data) {
            throw ConstEvalException.INSTANCE;
        }

        @Override
        public BufferObjectDataInput createObjectDataInput(byte[] data) {
            throw ConstEvalException.INSTANCE;
        }

        @Override
        public BufferObjectDataInput createObjectDataInput(byte[] data, int offset) {
            throw ConstEvalException.INSTANCE;
        }

        @Override
        public BufferObjectDataInput createObjectDataInput(Data data) {
            throw ConstEvalException.INSTANCE;
        }

        @Override
        public BufferObjectDataOutput createObjectDataOutput(int size) {
            throw ConstEvalException.INSTANCE;
        }

        @Override
        public BufferObjectDataOutput createObjectDataOutput() {
            throw ConstEvalException.INSTANCE;
        }

        @Override
        public InternalGenericRecord readAsInternalGenericRecord(Data data) {
            throw ConstEvalException.INSTANCE;
        }

        @Override
        public Schema extractSchemaFromData(@Nonnull Data data) {
            throw ConstEvalException.INSTANCE;
        }

        @Override
        public Schema extractSchemaFromObject(@Nonnull Object object) {
            throw ConstEvalException.INSTANCE;
        }

        @Override
        public boolean isCompactSerializable(Object object) {
            throw ConstEvalException.INSTANCE;
        }

        @Override
        public PortableContext getPortableContext() {
            throw ConstEvalException.INSTANCE;
        }

        @Override
        public ClassLoader getClassLoader() {
            throw ConstEvalException.INSTANCE;
        }

        @Override
        public ByteOrder getByteOrder() {
            throw ConstEvalException.INSTANCE;
        }

        @Override
        public byte getVersion() {
            throw ConstEvalException.INSTANCE;
        }

        @Override
        public <B extends Data> B toData(Object obj) {
            throw ConstEvalException.INSTANCE;
        }

        @Override
        public <B extends Data> B toDataWithSchema(Object obj) {
            throw ConstEvalException.INSTANCE;
        }

        @Override
        public <B extends Data> B toData(Object obj, PartitioningStrategy strategy) {
            throw ConstEvalException.INSTANCE;
        }

        @Override
        public <T> T toObject(Object data) {
            throw ConstEvalException.INSTANCE;
        }

        @Override
        public <T> T toObject(Object data, Class klazz) {
            throw ConstEvalException.INSTANCE;
        }

        @Override
        public ManagedContext getManagedContext() {
            throw ConstEvalException.INSTANCE;
        }

        @Override
        public <B extends Data> B trimSchema(Data data) {
            throw ConstEvalException.INSTANCE;
        }

    }
}
