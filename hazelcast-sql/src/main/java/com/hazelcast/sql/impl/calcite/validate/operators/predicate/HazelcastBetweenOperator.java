/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.calcite.validate.operators.predicate;

import com.hazelcast.sql.impl.ParameterConverter;
import com.hazelcast.sql.impl.calcite.validate.HazelcastCallBinding;
import com.hazelcast.sql.impl.calcite.validate.HazelcastSqlValidator;
import com.hazelcast.sql.impl.calcite.validate.operators.BetweenOperatorOperandTypeInference;
import com.hazelcast.sql.impl.calcite.validate.operators.common.HazelcastInfixOperator;
import com.hazelcast.sql.impl.calcite.validate.param.NumericPrecedenceParameterConverter;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeComparability;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.fun.SqlBetweenOperator.Flag;
import org.apache.calcite.sql.type.ComparableOperandTypeChecker;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlOperandTypeChecker.Consistency;

import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeUtils.toHazelcastType;
import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeUtils.withHigherPrecedence;

/*
 *  Grammar
 *  <between predicate> ::=
 *      <row value expression> [ NOT ] BETWEEN
 *      [ ASYMMETRIC | SYMMETRIC ]
 *      <left row value expression> AND <right row value expression>
 */
public final class HazelcastBetweenOperator extends HazelcastInfixOperator {
    public static final HazelcastBetweenOperator BETWEEN_ASYMMETRIC;
    public static final HazelcastBetweenOperator NOT_BETWEEN_ASYMMETRIC;
    public static final HazelcastBetweenOperator BETWEEN_SYMMETRIC;
    public static final HazelcastBetweenOperator NOT_BETWEEN_SYMMETRIC;

    private static final String[] BETWEEN_NAMES = {"BETWEEN ASYMMETRIC", "AND"};
    private static final String[] NOT_BETWEEN_NAMES = {"NOT BETWEEN ASYMMETRIC", "AND"};
    private static final String[] SYMMETRIC_BETWEEN_NAMES = {"BETWEEN SYMMETRIC", "AND"};
    private static final String[] SYMMETRIC_NOT_BETWEEN_NAMES = {"NOT BETWEEN SYMMETRIC", "AND"};

    private static final int PRECEDENCE = 32;
    private static final int OPERANDS = 3;

    static {
        BETWEEN_ASYMMETRIC = new HazelcastBetweenOperator(false, Flag.ASYMMETRIC, BETWEEN_NAMES);
        NOT_BETWEEN_ASYMMETRIC = new HazelcastBetweenOperator(true, Flag.ASYMMETRIC, NOT_BETWEEN_NAMES);
        BETWEEN_SYMMETRIC = new HazelcastBetweenOperator(false, Flag.SYMMETRIC, SYMMETRIC_BETWEEN_NAMES);
        NOT_BETWEEN_SYMMETRIC = new HazelcastBetweenOperator(true, Flag.SYMMETRIC, SYMMETRIC_NOT_BETWEEN_NAMES);
    }

    private final boolean negated;
    private final Flag flag;

    protected HazelcastBetweenOperator(boolean negated, Flag symmetricalFlag, String[] names) {
        super(names,
                SqlKind.BETWEEN,
                PRECEDENCE,
                ReturnTypes.BOOLEAN_NULLABLE,
                BetweenOperatorOperandTypeInference.INSTANCE,
                new ComparableOperandTypeChecker(3, RelDataTypeComparability.ALL, Consistency.COMPARE)
        );
        this.negated = negated;
        this.flag = symmetricalFlag;
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.of(OPERANDS);
    }

    @Override
    protected boolean checkOperandTypes(HazelcastCallBinding callBinding, boolean throwOnFailure) {
        assert callBinding.getOperandCount() == OPERANDS;
        for (int i = 0; i < OPERANDS; ++i) {
            RelDataType type = callBinding.getOperandType(i);
            // fast fail-forward path.
            if (type.getComparability().ordinal() < RelDataTypeComparability.ALL.ordinal()) {
                if (throwOnFailure) {
                    throw callBinding.newValidationSignatureError();
                } else {
                    return false;
                }
            }
        }

        HazelcastSqlValidator validator = callBinding.getValidator();
        RelDataType winningType = withHigherPrecedence(
                callBinding.getOperandType(0),  withHigherPrecedence(
                        callBinding.getOperandType(1),
                        callBinding.getOperandType(2)
                ));

        QueryDataType winnerQueryDataType = toHazelcastType(winningType.getSqlTypeName());

        // Set more flexible parameter converter that allows TINYINT/SMALLINT/INTEGER -> BIGINT conversions.
        if (winnerQueryDataType.getTypeFamily().isNumeric()) {
            setNumericParameterConverter(validator, callBinding.getCall().getOperandList().get(1), winnerQueryDataType);
            setNumericParameterConverter(validator, callBinding.getCall().getOperandList().get(2), winnerQueryDataType);
        }

        return true;
    }

    public Flag getFlag() {
        return flag;
    }

    public boolean isNegated() {
        return negated;
    }

    private void setNumericParameterConverter(HazelcastSqlValidator validator, SqlNode node, QueryDataType type) {
        if (node.getKind() == SqlKind.DYNAMIC_PARAM) {
            SqlDynamicParam node0 = (SqlDynamicParam) node;

            ParameterConverter converter = new NumericPrecedenceParameterConverter(
                    node0.getIndex(),
                    node.getParserPosition(),
                    type
            );

            validator.setParameterConverter(node0.getIndex(), converter);
        }
    }

}
