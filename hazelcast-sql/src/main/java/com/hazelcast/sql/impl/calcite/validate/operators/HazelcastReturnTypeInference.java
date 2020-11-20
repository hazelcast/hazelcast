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

package com.hazelcast.sql.impl.calcite.validate.operators;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.type.SqlReturnTypeInference;

import java.util.ArrayDeque;
import java.util.Deque;

public final class HazelcastReturnTypeInference implements SqlReturnTypeInference {

    private static final ThreadLocal<Deque<RelDataType>> QUEUE = new ThreadLocal<>();

    private final SqlReturnTypeInference delegate;

    private HazelcastReturnTypeInference(SqlReturnTypeInference delegate) {
        this.delegate = delegate;
    }

    public static HazelcastReturnTypeInference wrap(SqlReturnTypeInference delegate) {
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
        Deque<RelDataType> queue = QUEUE.get();

        if (queue == null) {
            queue = new ArrayDeque<>(2);

            QUEUE.set(queue);
        }

        queue.push(callType);
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
