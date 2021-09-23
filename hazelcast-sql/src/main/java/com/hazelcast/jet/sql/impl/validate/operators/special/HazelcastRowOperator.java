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

package com.hazelcast.jet.sql.impl.validate.operators.special;

import com.hazelcast.jet.sql.impl.validate.HazelcastCallBinding;
import com.hazelcast.jet.sql.impl.validate.operators.common.HazelcastSpecialOperator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlRowOperator;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.util.Pair;

import java.util.AbstractList;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Hazelcast equivalent of {@link SqlRowOperator}.
 */
public class HazelcastRowOperator extends HazelcastSpecialOperator {
    public HazelcastRowOperator() {
        super("ROW",
                SqlKind.ROW,
                MDX_PRECEDENCE,
                false,
                opBinding -> {
                    // The type of a ROW(e1, e2) expression is a record with the types
                    // {e1type, e2type}.  According to the standard, field names are
                    // implementation-defined.
                    return opBinding.getTypeFactory().createStructType(
                            new AbstractList<Entry<String, RelDataType>>() {
                                public Map.Entry<String, RelDataType> get(int index) {
                                    return Pair.of(
                                            SqlUtil.deriveAliasFromOrdinal(index),
                                            opBinding.getOperandType(index));
                                }

                                public int size() {
                                    return opBinding.getOperandCount();
                                }
                            });
                },
                InferTypes.RETURN_TYPE);
    }

    @Override
    protected boolean checkOperandTypes(HazelcastCallBinding callBinding, boolean throwOnFailure) {
        return true;
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.any();
    }

    @Override
    public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
        SqlUtil.unparseFunctionSyntax(this, writer, call, false);
    }
}
