package com.hazelcast.sql.impl.calcite;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.util.ReflectiveSqlOperatorTable;

public class HazelcastSqlOperatorTable extends ReflectiveSqlOperatorTable {
    private static HazelcastSqlOperatorTable INSTANCE = new HazelcastSqlOperatorTable();

    public static final SqlFunction LENGTH =
        new SqlFunction(
            "LENGTH",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.INTEGER_NULLABLE,
            null,
            OperandTypes.CHARACTER,
            SqlFunctionCategory.NUMERIC
        );

    static {
        INSTANCE.init();
    }

    public static HazelcastSqlOperatorTable instance() {
        return INSTANCE;
    }

    private HazelcastSqlOperatorTable() {
        // No-op.
    }
}
