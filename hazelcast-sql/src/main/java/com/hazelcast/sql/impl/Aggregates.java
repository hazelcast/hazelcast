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

package com.hazelcast.sql.impl;

import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.aggregation.Aggregators;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlKind;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;

public class Aggregates {

    private static final Set<SqlKind> SUPPORTED_AGGREGATIONS =
            EnumSet.of(SqlKind.COUNT, SqlKind.MIN, SqlKind.MAX, SqlKind.AVG, SqlKind.SUM);

    public static boolean isSupported(Aggregate aggregate) {
        // TODO: there is no fields provided by Calcite during optimization (bug?), how to check for fields?

        if (aggregate.getGroupType() != Aggregate.Group.SIMPLE) {
            return false;
        }

        if (isDistinct(aggregate)) {
            return true;
        }

        if (aggregate.getAggCallList().size() == 1 && aggregate.getGroupSets().size() == 1 && aggregate.getGroupSet().isEmpty()
                && aggregate.getRowType().getFieldCount() == 1) {
            // TODO: why aggregate.getGroupSets().size() == 1? is it a bug?
            return isSupported(aggregate.getAggCallList().get(0));
        }

        return false;
    }

    public static Aggregator convert(Aggregate aggregate) {
        List<RelDataTypeField> fields = aggregate.getInput().getRowType().getFieldList();

        if (isDistinct(aggregate)) {
            String attribute = fields.get(aggregate.getGroupSet().nextSetBit(0)).getName();
            return Aggregators.distinct(attribute);
        }

        if (aggregate.getAggCallList().size() == 1 && aggregate.getGroupSets().size() == 1 && aggregate.getGroupSet().isEmpty()
                && aggregate.getRowType().getFieldCount() == 1) {
            // TODO: why aggregate.getGroupSets().size() == 1? is it a bug?

            AggregateCall call = aggregate.getAggCallList().get(0);
            String attribute = call.getArgList().size() == 0 ? null : fields.get(call.getArgList().get(0)).getName();

            switch (call.getAggregation().kind) {
                case COUNT:
                    return attribute == null ? Aggregators.count() : Aggregators.count(attribute);
                case MAX:
                    assert attribute != null;
                    return Aggregators.comparableMax(attribute);
                case MIN:
                    assert attribute != null;
                    return Aggregators.comparableMin(attribute);
                case AVG:
                    assert attribute != null;
                    return Aggregators.numberAvg(attribute);
                case SUM:
                    assert attribute != null;
                    return Aggregators.floatingPointSum(attribute);
            }
        }

        throw new IllegalStateException();
    }

    public static boolean isDistinct(Aggregate aggregate) {
        return aggregate.getAggCallList().isEmpty() && aggregate.getGroupSets().size() == 1
                && aggregate.getGroupSet().cardinality() == 1 && aggregate.getRowType().getFieldCount() == 1;
    }

    private Aggregates() {
    }

    private static boolean isSupported(AggregateCall call) {
        if (call.getArgList().size() > 1 || call.isDistinct()) {
            return false;
        }

        return call.getAggregation().kind.belongsTo(SUPPORTED_AGGREGATIONS);
    }

}
