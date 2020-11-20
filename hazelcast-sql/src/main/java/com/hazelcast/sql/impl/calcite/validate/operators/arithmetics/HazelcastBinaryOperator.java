package com.hazelcast.sql.impl.calcite.validate.operators.arithmetics;

import com.hazelcast.sql.impl.calcite.validate.binding.SqlCallBindingManualOverride;
import com.hazelcast.sql.impl.calcite.validate.binding.SqlCallBindingOverride;
import com.hazelcast.sql.impl.calcite.validate.operand.CompositeOperandChecker;
import com.hazelcast.sql.impl.calcite.validate.operand.TypedOperandChecker;
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastIntegerType;
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastTrackingReturnTypeInference;
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeSystem;
import com.hazelcast.sql.impl.calcite.validate.types.ReplaceUnknownOperandTypeInference;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlReturnTypeInference;

import static org.apache.calcite.sql.type.SqlTypeName.BIGINT;

public final class HazelcastBinaryOperator extends SqlBinaryOperator implements SqlCallBindingManualOverride {

    public static final HazelcastBinaryOperator PLUS = new HazelcastBinaryOperator(SqlStdOperatorTable.PLUS);
    public static final HazelcastBinaryOperator MINUS = new HazelcastBinaryOperator(SqlStdOperatorTable.MINUS);
    public static final HazelcastBinaryOperator MULTIPLY = new HazelcastBinaryOperator(SqlStdOperatorTable.MULTIPLY);
    public static final HazelcastBinaryOperator DIVIDE = new HazelcastBinaryOperator(SqlStdOperatorTable.DIVIDE);

    private HazelcastBinaryOperator(SqlBinaryOperator base) {
        super(
            base.getName(),
            base.getKind(),
            base.getLeftPrec(),
            true,
            new ReturnTypeInference(),
            new ReplaceUnknownOperandTypeInference(BIGINT),
            null
        );
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.of(2);
    }

    @Override
    public SqlSyntax getSyntax() {
        return SqlSyntax.BINARY;
    }

    // TODO: This might be called before the inferUnknownOperands routine!
    @Override
    public boolean checkOperandTypes(SqlCallBinding binding, boolean throwOnFailure) {
        SqlCallBindingOverride bindingOverride = new SqlCallBindingOverride(binding);

        RelDataType firstType = bindingOverride.getOperandType(0);
        RelDataType secondType = bindingOverride.getOperandType(1);

        if (!isNumeric(firstType) || !isNumeric(secondType)) {
            if (throwOnFailure) {
                throw bindingOverride.newValidationSignatureError();
            } else {
                return false;
            }
        }

        RelDataType type = HazelcastTypeSystem.withHigherPrecedence(firstType, secondType);

        switch (kind) {
            case PLUS:
            case MINUS:
                if (HazelcastTypeSystem.isInteger(type)) {
                    int bitWidth = HazelcastIntegerType.bitWidthOf(type) + 1;

                    type = HazelcastIntegerType.of(bitWidth, type.isNullable());
                }

                break;

            case TIMES:
                if (HazelcastTypeSystem.isInteger(firstType) && HazelcastTypeSystem.isInteger(secondType)) {
                    int bitWidth = HazelcastIntegerType.bitWidthOf(firstType) + HazelcastIntegerType.bitWidthOf(secondType);

                    type = HazelcastIntegerType.of(bitWidth, type.isNullable());
                }

                break;

            default:
                assert kind == SqlKind.DIVIDE;
        }

        TypedOperandChecker checker = TypedOperandChecker.forType(type);

        return new CompositeOperandChecker(
            checker,
            checker
        ).check(bindingOverride, throwOnFailure);
    }

    private static boolean isNumeric(RelDataType type) {
        switch (type.getSqlTypeName()) {
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case DECIMAL:
            case REAL:
            case FLOAT:
            case DOUBLE:
                return true;

            default:
                return false;
        }
    }

    // TODO: Apply to all operators
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
