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
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.converter.LongConverter;

/**
 * Collector for AVG.
 */
public class AverageAggregateCollector extends AggregateCollector {
    /** Result type. */
    private final QueryDataType resType;

    /** Sum. */
    private final SumAggregateCollector sum;

    /** Average. */
    private final CountAggregateCollector count;

    public AverageAggregateCollector(QueryDataType resType, boolean distinct) {
        super(distinct);

        sum = new SumAggregateCollector(resType, false);
        count = new CountAggregateCollector(false);

        this.resType = resType;
    }

    @Override
    protected void collect0(Object operandValue, QueryDataType operandType) {
        collectMany(operandValue, operandType, 1);
    }

    public void collectMany(Object operandValue, QueryDataType operandType, long cnt) {
        sum.collect(operandValue, operandType);
        count.collectMany(cnt);
    }

    @Override
    public Object reduce() {
        if (this.resType == null) {
            return 0.0d;
        }

        double sum0 = resType.getConverter().asDouble(sum.reduce());
        double count0 = LongConverter.INSTANCE.asDouble(count.reduce());

        return sum0 / count0;
    }
}
