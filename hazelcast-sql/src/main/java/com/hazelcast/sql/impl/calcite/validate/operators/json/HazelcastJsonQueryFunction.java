package com.hazelcast.sql.impl.calcite.validate.operators.json;

import com.hazelcast.sql.impl.calcite.validate.HazelcastCallBinding;
import com.hazelcast.sql.impl.calcite.validate.operand.TypedOperandChecker;
import com.hazelcast.sql.impl.calcite.validate.operators.ReplaceUnknownOperandTypeInference;
import com.hazelcast.sql.impl.calcite.validate.operators.common.HazelcastFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlTypeName;

public class HazelcastJsonQueryFunction extends HazelcastFunction {
    public static final HazelcastJsonQueryFunction INSTANCE = new HazelcastJsonQueryFunction();

    public HazelcastJsonQueryFunction() {
        super(
                "JSON_QUERY",
                SqlKind.OTHER_FUNCTION,
                ReturnTypes.ARG0,
                new ReplaceUnknownOperandTypeInference(SqlTypeName.VARCHAR),
                SqlFunctionCategory.SYSTEM
        );
    }

    @Override
    protected boolean checkOperandTypes(final HazelcastCallBinding callBinding, final boolean throwOnFailure) {
        // TODO correct checks
        return TypedOperandChecker.VARCHAR.check(callBinding, throwOnFailure, 1);
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.from(2);
    }
}
