/*
 * Copyright 2023 Hazelcast Inc.
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

package com.hazelcast.jet.sql.impl.opt.metadata;

import com.hazelcast.jet.datamodel.Tuple2;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

class PartitionStrategyConditionExtractor {
    Map<String, RexNode> completenessMap;

    PartitionStrategyConditionExtractor() {
        this.completenessMap = new HashMap<>();
    }

    // Tuple2(mapName, Map(columnName -> condition))
    public Tuple2<String, Map<String, RexNode>> extractCondition(String mapName, RexCall call) {
        // $1 = 1e
        switch (call.getKind()) {
            // TODO: more cases.
            case EQUALS:
                assert call.getOperands().size() == 2;
                final RexInputRef inputRef = extractInputRef(call);
                final RexNode constantExpr = extractConstantExpression(call);
                if (inputRef == null || constantExpr == null) {
                    return Tuple2.tuple2(mapName, Collections.emptyMap());
                }
                completenessMap.put(inputRef.getName(), call);

        }
        return Tuple2.tuple2(mapName, completenessMap);
    }

    private RexInputRef extractInputRef(final RexCall call) {
        // only works for EQUALS
        assert call.isA(SqlKind.EQUALS);
        if (call.getOperands().get(0) instanceof RexInputRef) {
            return (RexInputRef) call.getOperands().get(0);
        }

        if (call.getOperands().get(1) instanceof RexInputRef) {
            return (RexInputRef) call.getOperands().get(1);
        }

        return null;
    }

    private RexNode extractConstantExpression(final RexCall call) {
        assert call.isA(SqlKind.EQUALS);
        if (call.getOperands().get(0) instanceof RexDynamicParam) {
            return call.getOperands().get(0);
        }

        if (call.getOperands().get(1) instanceof RexDynamicParam) {
            return call.getOperands().get(1);
        }

        if (call.getOperands().get(0) instanceof RexLiteral) {
            return call.getOperands().get(0);
        }

        if (call.getOperands().get(1) instanceof RexLiteral) {
            return call.getOperands().get(1);
        }

        return null;
    }
}