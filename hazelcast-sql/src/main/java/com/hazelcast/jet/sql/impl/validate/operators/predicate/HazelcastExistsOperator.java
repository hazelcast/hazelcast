package com.hazelcast.jet.sql.impl.validate.operators.predicate;

import com.hazelcast.jet.sql.impl.validate.HazelcastCallBinding;
import com.hazelcast.jet.sql.impl.validate.operators.common.HazelcastPrefixOperator;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;

public class HazelcastExistsOperator extends HazelcastPrefixOperator {
    public static final HazelcastExistsOperator INSTANCE = new HazelcastExistsOperator();

    public HazelcastExistsOperator() {
        super(
                "EXISTS",
                SqlKind.EXISTS,
                SqlStdOperatorTable.EXISTS.getLeftPrec(),
                ReturnTypes.BOOLEAN,
                null
        );
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.any();
    }

    @Override
    protected boolean checkOperandTypes(HazelcastCallBinding callBinding, boolean throwOnFailure) {
        return true;
    }
}
