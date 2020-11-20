package com.hazelcast.sql.impl.calcite.validate.operators.arithmetics;

import com.hazelcast.sql.impl.calcite.validate.binding.SqlCallBindingManualOverride;
import com.hazelcast.sql.impl.calcite.validate.binding.SqlCallBindingOverride;
import com.hazelcast.sql.impl.calcite.validate.operand.TypedOperandChecker;
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastIntegerType;
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastTrackingReturnTypeInference;
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeSystem;
import com.hazelcast.sql.impl.calcite.validate.types.ReplaceUnknownOperandTypeInference;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlPrefixOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlReturnTypeInference;

import static org.apache.calcite.sql.type.SqlTypeName.BIGINT;

public final class HazelcastUnaryOperator extends SqlPrefixOperator implements SqlCallBindingManualOverride {

    public static final HazelcastUnaryOperator PLUS = new HazelcastUnaryOperator(SqlStdOperatorTable.UNARY_PLUS, false);
    public static final HazelcastUnaryOperator MINUS = new HazelcastUnaryOperator(SqlStdOperatorTable.UNARY_MINUS, true);

    private final boolean extend;

    private HazelcastUnaryOperator(
        SqlPrefixOperator base,
        boolean extend
    ) {
        super(
            base.getName(),
            base.getKind(),
            base.getLeftPrec(),
            new ReturnTypeInference(),
            new ReplaceUnknownOperandTypeInference(BIGINT),
            null
        );

        this.extend = extend;
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.of(1);
    }

    @Override
    public boolean checkOperandTypes(SqlCallBinding binding, boolean throwOnFailure) {
        SqlCallBindingOverride bindingOverride = new SqlCallBindingOverride(binding);

        RelDataType operandType = bindingOverride.getOperandType(0);

        if (HazelcastTypeSystem.isInteger(operandType) && extend) {
            int bitWidth = HazelcastIntegerType.bitWidthOf(operandType);

            operandType = HazelcastIntegerType.of(bitWidth + 1, operandType.isNullable());
        }

        TypedOperandChecker checker = TypedOperandChecker.forType(operandType);

        if (checker.isNumeric()) {
            return checker.check(bindingOverride, throwOnFailure, 0);
        }

        if (throwOnFailure) {
            throw bindingOverride.newValidationSignatureError();
        } else {
            return false;
        }
    }

    private static class ReturnTypeInference implements SqlReturnTypeInference {
        @Override
        public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
            RelDataType knownType = HazelcastTrackingReturnTypeInference.peek();

            if (knownType != null) {
                return knownType;
            }

            return opBinding.getOperandType(0);
        }
    }
}
