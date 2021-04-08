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

import com.hazelcast.sql.impl.calcite.validate.HazelcastCallBinding;
import com.hazelcast.sql.impl.calcite.validate.operators.common.HazelcastInfixOperator;
import org.apache.calcite.rel.type.RelDataTypeComparability;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlBetweenOperator.Flag;
import org.apache.calcite.sql.type.ComparableOperandTypeChecker;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;

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

    static {
        BETWEEN_ASYMMETRIC = new HazelcastBetweenOperator(false, Flag.ASYMMETRIC);
        NOT_BETWEEN_ASYMMETRIC = new HazelcastBetweenOperator(true, Flag.ASYMMETRIC);
        BETWEEN_SYMMETRIC = new HazelcastBetweenOperator(false, Flag.SYMMETRIC);
        NOT_BETWEEN_SYMMETRIC = new HazelcastBetweenOperator(true, Flag.SYMMETRIC);
    }

    private final boolean negated;
    private final Flag flag;

    protected HazelcastBetweenOperator(boolean negated, Flag symmetricalFlag) {
        super(negated
                ? (symmetricalFlag == Flag.ASYMMETRIC) ? NOT_BETWEEN_NAMES : SYMMETRIC_NOT_BETWEEN_NAMES
                : (symmetricalFlag == Flag.ASYMMETRIC) ? BETWEEN_NAMES : SYMMETRIC_BETWEEN_NAMES,
            SqlKind.BETWEEN,
            PRECEDENCE,
            ReturnTypes.BOOLEAN_NULLABLE,
            InferTypes.FIRST_KNOWN,
            new ComparableOperandTypeChecker(3, RelDataTypeComparability.ALL,
                SqlOperandTypeChecker.Consistency.COMPARE));
        this.negated = negated;
        this.flag = symmetricalFlag;
    }

    @Override
    protected boolean checkOperandTypes(HazelcastCallBinding callBinding, boolean throwOnFailure) {
        return false;
    }

    public Flag getFlag() {
        return flag;
    }

    public boolean isNegated() {
        return negated;
    }
}
