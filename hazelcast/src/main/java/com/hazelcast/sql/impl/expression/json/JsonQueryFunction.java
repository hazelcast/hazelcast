package com.hazelcast.sql.impl.expression.json;

import com.hazelcast.internal.util.StringUtil;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.expression.BiExpression;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.expression.VariExpression;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.jayway.jsonpath.JsonPath;

import static com.hazelcast.internal.util.StringUtil.isNullOrEmpty;

public class JsonQueryFunction extends VariExpression<String> implements IdentifiedDataSerializable {

    public JsonQueryFunction() {}

    private JsonQueryFunction(Expression<?>[] operands) {
        super(operands);
    }

    public static JsonQueryFunction create(Expression<?>[] operands) {
        return new JsonQueryFunction(operands);
    }

    @Override
    public int getFactoryId() {
        return SqlDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SqlDataSerializerHook.JSON_QUERY;
    }

    @Override
    public String eval(final Row row, final ExpressionEvalContext context) {
        final String json = (String) operands[0].eval(row, context);
        final String path = (String) operands[1].eval(row, context);

        if (isNullOrEmpty(json) || isNullOrEmpty(path)) {
            return null;
        }

        return JsonPath.read(json, path).toString();
    }

    @Override
    public QueryDataType getType() {
        return QueryDataType.VARCHAR;
    }
}
