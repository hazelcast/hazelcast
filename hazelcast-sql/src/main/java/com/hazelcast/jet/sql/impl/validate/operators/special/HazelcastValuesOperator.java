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
import com.hazelcast.jet.sql.impl.validate.operators.typeinference.ReplaceUnknownOperandTypeInference;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlValuesOperator;
import org.apache.calcite.sql.SqlWriter;

import static org.apache.calcite.sql.type.SqlTypeName.BIGINT;

/**
 * Hazelcast equivalent of {@link SqlValuesOperator}.
 */
public class HazelcastValuesOperator extends HazelcastSpecialOperator {
    public HazelcastValuesOperator() {
        super("VALUES", SqlKind.VALUES, 2, true,
                b -> {
                    throw new UnsupportedOperationException();
                },
                new ReplaceUnknownOperandTypeInference(BIGINT));
    }

    @Override
    protected boolean checkOperandTypes(HazelcastCallBinding callBinding, boolean throwOnFailure) {
        throw new UnsupportedOperationException("never called for VALUES");
    }

    @Override
    public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
        SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.VALUES, "VALUES", "");
        for (SqlNode operand : call.getOperandList()) {
            writer.sep(",");
            operand.unparse(writer, 0, 0);
        }
        writer.endList(frame);
    }
}
