package com.hazelcast.jet.sql.impl.expression.json;

import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.jet.sql.impl.JetSqlSerializerHook;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.expression.UniExpressionWithType;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;

public class ParseJsonFunction extends UniExpressionWithType<HazelcastJsonValue> implements IdentifiedDataSerializable {

    public ParseJsonFunction() { }

    private ParseJsonFunction(Expression<?> operand) {
        super(operand, QueryDataType.JSON);
    }

    public static ParseJsonFunction create(Expression<?> operand) {
        return new ParseJsonFunction(operand);
    }

    @Override
    public int getFactoryId() {
        return JetSqlSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return JetSqlSerializerHook.PARSE_JSON;
    }

    @Override
    public HazelcastJsonValue eval(final Row row, final ExpressionEvalContext context) {
        final String operand = (String) this.operand.eval(row, context);
        return new HazelcastJsonValue(operand);
    }

    @Override
    public QueryDataType getType() {
        return QueryDataType.JSON;
    }
}
