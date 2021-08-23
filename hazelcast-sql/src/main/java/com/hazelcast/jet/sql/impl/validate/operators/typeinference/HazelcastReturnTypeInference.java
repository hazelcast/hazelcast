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

package com.hazelcast.jet.sql.impl.validate.operators.typeinference;

import com.hazelcast.jet.sql.impl.HazelcastSqlToRelConverter;
import com.hazelcast.jet.sql.impl.validate.operators.common.HazelcastAggFunction;
import com.hazelcast.jet.sql.impl.validate.operators.common.HazelcastBinaryOperator;
import com.hazelcast.jet.sql.impl.validate.operators.common.HazelcastFunction;
import com.hazelcast.jet.sql.impl.validate.operators.common.HazelcastPostfixOperator;
import com.hazelcast.jet.sql.impl.validate.operators.common.HazelcastPrefixOperator;
import com.hazelcast.jet.sql.impl.validate.operators.common.HazelcastSpecialOperator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.type.SqlReturnTypeInference;

import javax.annotation.Nonnull;
import java.util.ArrayDeque;
import java.util.Deque;

/**
 * The inference strategy that allows to transfer the return type info between validation and conversion phases.
 * <p>
 * When doing sql-to-rel conversion, Apache Calcite ignores information about the inferred return types from the validation
 * phase. To fix this, we intercept {@link SqlCall} conversions in the {@link HazelcastSqlToRelConverter}, lookup the
 * real return type, and put it to thread-local stack. Then, when the return type inference is invoked again during the
 * conversion phase, it peeks the thread-local stack, rather than trying to infer again.
 * <p>
 * In order for this workaround to work, every operator must have {@code HazelcastReturnTypeInference} as a
 * return type inference strategy. This is controlled by the automated test. To simplify the development of operators,
 * we create a number of base operator classes that set the required return type inference: {@link HazelcastFunction},
 * {@link HazelcastPrefixOperator}, {@link HazelcastPostfixOperator}, {@link HazelcastBinaryOperator},
 * {@link HazelcastSpecialOperator}, {@link HazelcastAggFunction}. Every defined operator should extend one of these
 * classes.
 */
public final class HazelcastReturnTypeInference implements SqlReturnTypeInference {

    private static final ThreadLocal<Deque<RelDataType>> QUEUE = ThreadLocal.withInitial(() -> new ArrayDeque<>(2));

    private final SqlReturnTypeInference delegate;

    private HazelcastReturnTypeInference(SqlReturnTypeInference delegate) {
        this.delegate = delegate;
    }

    @Nonnull
    public static HazelcastReturnTypeInference wrap(@Nonnull SqlReturnTypeInference delegate) {
        return new HazelcastReturnTypeInference(delegate);
    }

    @Override
    public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
        RelDataType type = peek();

        if (type != null) {
            return type;
        }

        return delegate.inferReturnType(opBinding);
    }

    public static void push(RelDataType callType) {
        QUEUE.get().push(callType);
    }

    public static void pop() {
        Deque<RelDataType> queue = QUEUE.get();

        assert queue != null;

        RelDataType type = queue.poll();

        assert type != null;

        if (queue.isEmpty()) {
            QUEUE.remove();
        }
    }

    private static RelDataType peek() {
        Deque<RelDataType> queue = QUEUE.get();

        return queue != null ? queue.peek() : null;
    }
}
