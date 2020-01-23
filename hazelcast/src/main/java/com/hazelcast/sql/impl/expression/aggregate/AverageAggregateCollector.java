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

package com.hazelcast.sql.impl.expression.aggregate;

import com.hazelcast.sql.impl.exec.agg.AggregateCollector;
import com.hazelcast.sql.impl.type.DataType;
import com.hazelcast.sql.impl.type.accessor.Converter;
import com.hazelcast.sql.impl.type.accessor.LongConverter;

/**
 * Collector for AVG.
 */
public class AverageAggregateCollector extends AggregateCollector {
    /** Sum. */
    private final SumAggregateCollector sum = new SumAggregateCollector(false);

    /** Average. */
    private final CountAggregateCollector count = new CountAggregateCollector(false);

    /** Result type. */
    private DataType resType;

    public AverageAggregateCollector(boolean distinct) {
        super(distinct);
    }

    @Override
    protected void collect0(Object operandValue, DataType operandType, DataType resType) {
        collectMany(operandValue, operandType, resType, 1);
    }

    public void collectMany(Object operandValue, DataType operandType, DataType resType, long cnt) {
        if (this.resType == null) {
            this.resType = resType;
        }

        sum.collect(operandValue, operandType, resType);
        count.collectMany(cnt);
    }

    @Override
    public Object reduce() {
        if (this.resType == null) {
            return 0.0d;
        }

        Converter converter = resType.getConverter();

        double sum0 = converter.asDouble(sum.reduce());
        double count0 = LongConverter.INSTANCE.asDouble(count.reduce());

        return sum0 / count0;
    }
}
