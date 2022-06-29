package com.hazelcast.jet.sql.impl.aggregate.function;

import com.hazelcast.jet.sql.impl.validate.HazelcastCallBinding;
import com.hazelcast.jet.sql.impl.validate.operators.common.HazelcastAggFunction;
import com.hazelcast.jet.sql.impl.validate.operators.typeinference.ReplaceUnknownOperandTypeInference;
import com.hazelcast.jet.sql.impl.validate.types.HazelcastJsonType;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlJsonConstructorNullClause;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Optionality;

public class HazelcastJsonArrayAggFunction extends HazelcastAggFunction {
    // public static final HazelcastJsonArrayAggFunction INSTANCE = new HazelcastJsonArrayAggFunction();
    public static final HazelcastJsonArrayAggFunction ABSENT_ON_NULL_INSTANCE = new HazelcastJsonArrayAggFunction(SqlJsonConstructorNullClause.ABSENT_ON_NULL);
    public static final HazelcastJsonArrayAggFunction NULL_ON_NULL_INSTANCE = new HazelcastJsonArrayAggFunction(SqlJsonConstructorNullClause.NULL_ON_NULL);

    private SqlJsonConstructorNullClause nullClause;

    protected HazelcastJsonArrayAggFunction(SqlJsonConstructorNullClause nullClause) {
        super(
                "JSON_ARRAYAGG" + "_" + nullClause.name(),
                SqlKind.JSON_ARRAYAGG,
                opBinding -> HazelcastJsonType.create(false),
                new ReplaceUnknownOperandTypeInference(SqlTypeName.ANY),
                null,
                SqlFunctionCategory.SYSTEM,
                true,
                false,
                Optionality.OPTIONAL
        );
        this.nullClause = nullClause;
    }

    @Override
    protected boolean checkOperandTypes(HazelcastCallBinding callBinding, boolean throwOnFailure) {
        return true;
    }

    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.from(1);
    }

    public SqlJsonConstructorNullClause getNullClause() {
        return nullClause;
    }
}
