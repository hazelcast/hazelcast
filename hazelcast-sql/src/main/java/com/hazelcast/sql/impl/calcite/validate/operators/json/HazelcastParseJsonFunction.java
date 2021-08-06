package com.hazelcast.sql.impl.calcite.validate.operators.json;

import com.hazelcast.sql.impl.calcite.validate.HazelcastCallBinding;
import com.hazelcast.sql.impl.calcite.validate.operand.TypedOperandChecker;
import com.hazelcast.sql.impl.calcite.validate.operators.ReplaceUnknownOperandTypeInference;
import com.hazelcast.sql.impl.calcite.validate.operators.common.HazelcastFunction;
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastJsonType;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlTypeName;

public class HazelcastParseJsonFunction extends HazelcastFunction {
    public static final HazelcastParseJsonFunction INSTANCE = new HazelcastParseJsonFunction();
    public HazelcastParseJsonFunction() {
        super(
                "PARSE_JSON",
                SqlKind.OTHER_FUNCTION,
                opBinding -> HazelcastJsonType.INSTANCE,
                new ReplaceUnknownOperandTypeInference(SqlTypeName.VARCHAR),
                SqlFunctionCategory.SYSTEM
        );
    }

    @Override
    protected boolean checkOperandTypes(final HazelcastCallBinding callBinding, final boolean throwOnFailure) {
        return TypedOperandChecker.VARCHAR.check(callBinding, throwOnFailure, 0);
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.from(1);
    }
}
