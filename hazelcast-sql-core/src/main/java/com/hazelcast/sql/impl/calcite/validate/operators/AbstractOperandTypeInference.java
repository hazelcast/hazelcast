package com.hazelcast.sql.impl.calcite.validate.operators;

import com.hazelcast.sql.impl.calcite.validate.HazelcastCallBinding;
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeUtils;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;

import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeUtils.createType;
import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeUtils.toHazelcastType;

public abstract class AbstractOperandTypeInference<S extends AbstractOperandTypeInference.State> implements SqlOperandTypeInference {
    @Override
    public void inferOperandTypes(SqlCallBinding binding, RelDataType returnType, RelDataType[] operandTypes) {
        precondition(operandTypes, binding);

        S localState = createLocalState();
        // Check if we have parameters. If yes, we will upcast integer literals to BIGINT as explained below
        boolean hasParameters = binding.operands().stream().anyMatch((operand) -> operand.getKind() == SqlKind.DYNAMIC_PARAM);

        RelDataType knownType = null;

        for (int i = 0; i < binding.getOperandCount(); i++) {
            RelDataType operandType = binding.getOperandType(i);

            if (operandType.getSqlTypeName() == SqlTypeName.NULL) {
                // Will resolve operand type at this index later.
                localState.update(i);
            } else {
                if (hasParameters && toHazelcastType(operandType.getSqlTypeName()).getTypeFamily().isNumericInteger()) {
                    // If we are here, the operands are a parameter and a numeric expression.
                    // We upcast the type of the numeric expression to BIGINT, so that an expression `1 > ?` is resolved to
                    // `(BIGINT)1 > (BIGINT)?` rather than `(TINYINT)1 > (TINYINT)?`
                    operandType = createType(
                            binding.getTypeFactory(),
                            SqlTypeName.BIGINT,
                            operandType.isNullable()
                    );
                }

                operandTypes[i] = operandType;

                if (knownType == null) {
                    knownType = operandType;
                } else {
                    knownType = HazelcastTypeUtils.withHigherPrecedence(knownType, operandType);
                }
            }
        }

        // If we have [UNKNOWN, UNKNOWN] operands, throw a signature error, since we cannot deduce the return type
        if (knownType == null) {
            throw new HazelcastCallBinding(binding).newValidationSignatureError();
        }

        updateUnresolvedTypes(binding, knownType, operandTypes, localState);
    }

    public interface State {
        void update(int index);
    }

    protected abstract S createLocalState();

    protected abstract void precondition(RelDataType[] operandTypes, SqlCallBinding binding);

    protected abstract void updateUnresolvedTypes(SqlCallBinding binding, RelDataType knownType, RelDataType[] operandTypes, S state);
}
