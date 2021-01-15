package com.hazelcast.sql.impl.expression;

import com.hazelcast.sql.SqlColumnType;
import org.junit.Test;

public class CaseOperationIntegrationTest extends ExpressionTestSupport {
    @Test
    public void dummyCase() {
        put(1);

        String sql = "select case when 1 = 1 then 1 else 2 end from map";

        checkValue0(sql, SqlColumnType.TINYINT, (byte)1);
    }
}
