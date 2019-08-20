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

package com.hazelcast.sql.impl.expression.aggregate;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.sql.impl.exec.AggregateExec;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.type.DataType;

import java.io.IOException;

/**
 * Common parent for all aggregate accumulators.
 */
public abstract class AggregateAccumulator<T> implements Expression<T> {
    /** Accumulator: sum. */
    public static final int TYPE_SUM = 1;

    /** Accumulator: count. */
    public static final int TYPE_COUNT = 2;

    /** Distinct flag. */
    protected boolean distinct;

    /** Result type. */
    protected transient DataType resType;

    /** Parent executor. */
    protected transient AggregateExec parent;

    protected AggregateAccumulator() {
        // No-op.
    }

    protected AggregateAccumulator(boolean distinct) {
        this.distinct = distinct;
    }

    public void setup(AggregateExec parent) {
        this.parent = parent;
    }

    /**
     * Get the final result and reset the state.
     *
     * @return Final result.
     */
    protected abstract T reduceAndReset();

    @Override
    public DataType getType() {
        return DataType.notNullOrLate(resType);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeBoolean(distinct);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        distinct = in.readBoolean();
    }
}
