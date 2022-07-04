/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.expression.math;

import com.hazelcast.jet.sql.impl.expression.ExpressionTestSupport;
import com.hazelcast.jet.sql.impl.support.expressions.ExpressionBiValue;
import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.sql.impl.SqlErrorCode;
import org.junit.Test;

import java.math.BigDecimal;

import static com.hazelcast.sql.SqlColumnType.BIGINT;
import static com.hazelcast.sql.SqlColumnType.BOOLEAN;
import static com.hazelcast.sql.SqlColumnType.DECIMAL;
import static com.hazelcast.sql.SqlColumnType.DOUBLE;
import static com.hazelcast.sql.SqlColumnType.INTEGER;
import static com.hazelcast.sql.SqlColumnType.NULL;
import static com.hazelcast.sql.SqlColumnType.OBJECT;
import static com.hazelcast.sql.SqlColumnType.SMALLINT;
import static com.hazelcast.sql.SqlColumnType.TINYINT;
import static com.hazelcast.sql.SqlColumnType.VARCHAR;
import static org.apache.calcite.sql.type.SqlTypeName.UNKNOWN;

public abstract class ArithmeticOperatorIntegrationTest extends ExpressionTestSupport {

    protected abstract String operator();

    @Test
    public void testVarchar() {
        checkUnsupportedForAllTypesCommute(CHAR_VAL, VARCHAR);
        checkUnsupportedForAllTypesCommute(STRING_VAL, VARCHAR);
    }

    @Test
    public void testBoolean() {
        checkUnsupportedForAllTypesCommute(BOOLEAN_VAL, BOOLEAN);
    }

    @Test
    public void testObject() {
        checkUnsupportedForAllTypesCommute(OBJECT_VAL, OBJECT);
    }

    @Test
    public void testParameterParameter() {
        put(1);

        checkFailure0(sql("?", "?"), SqlErrorCode.PARSING, signatureError(UNKNOWN, UNKNOWN));
    }

    @Test
    public void testNullLiteral() {
        put(1);

        checkFailure0(sql("null", "null"), SqlErrorCode.PARSING, signatureError(NULL, NULL));

        checkFailure0(sql("'foo'", "null"), SqlErrorCode.PARSING, signatureError(VARCHAR, VARCHAR));
        checkFailure0(sql("null", "'foo'"), SqlErrorCode.PARSING, signatureError(VARCHAR, VARCHAR));

        checkFailure0(sql("true", "null"), SqlErrorCode.PARSING, signatureError(BOOLEAN, BOOLEAN));
        checkFailure0(sql("null", "true"), SqlErrorCode.PARSING, signatureError(BOOLEAN, BOOLEAN));

        checkValue0(sql("null", 1), TINYINT, null);
        checkValue0(sql(1, "null"), TINYINT, null);

        checkValue0(sql("null", Byte.MAX_VALUE), SMALLINT, null);
        checkValue0(sql(Byte.MAX_VALUE, "null"), SMALLINT, null);

        checkValue0(sql("null", Short.MAX_VALUE), INTEGER, null);
        checkValue0(sql(Short.MAX_VALUE, "null"), INTEGER, null);

        checkValue0(sql("null", Integer.MAX_VALUE), BIGINT, null);
        checkValue0(sql(Integer.MAX_VALUE, "null"), BIGINT, null);

        checkValue0(sql("null", Long.MAX_VALUE), BIGINT, null);
        checkValue0(sql(Long.MAX_VALUE, "null"), BIGINT, null);

        checkValue0(sql("null", "1.1"), DECIMAL, null);
        checkValue0(sql("1.1", "null"), DECIMAL, null);

        checkValue0(sql("null", "1.1E1"), DOUBLE, null);
        checkValue0(sql("1.1E1", "null"), DOUBLE, null);
    }

    protected void checkUnsupportedForAllTypesCommute(Object field1, SqlColumnType type1) {
        checkSignatureErrorCommute(field1, CHAR_VAL, type1, SqlColumnType.VARCHAR);
        checkSignatureErrorCommute(field1, STRING_VAL, type1, SqlColumnType.VARCHAR);
        checkSignatureErrorCommute(field1, BYTE_VAL, type1, SqlColumnType.TINYINT);
        checkSignatureErrorCommute(field1, SHORT_VAL, type1, SqlColumnType.SMALLINT);
        checkSignatureErrorCommute(field1, INTEGER_VAL, type1, SqlColumnType.INTEGER);
        checkSignatureErrorCommute(field1, LONG_VAL, type1, SqlColumnType.BIGINT);
        checkSignatureErrorCommute(field1, BIG_INTEGER_VAL, type1, SqlColumnType.DECIMAL);
        checkSignatureErrorCommute(field1, BIG_DECIMAL_VAL, type1, SqlColumnType.DECIMAL);
        checkSignatureErrorCommute(field1, FLOAT_VAL, type1, SqlColumnType.REAL);
        checkSignatureErrorCommute(field1, DOUBLE_VAL, type1, SqlColumnType.DOUBLE);
        checkSignatureErrorCommute(field1, LOCAL_DATE_VAL, type1, SqlColumnType.DATE);
        checkSignatureErrorCommute(field1, LOCAL_TIME_VAL, type1, SqlColumnType.TIME);
        checkSignatureErrorCommute(field1, LOCAL_DATE_TIME_VAL, type1, SqlColumnType.TIMESTAMP);
        checkSignatureErrorCommute(field1, OFFSET_DATE_TIME_VAL, type1, SqlColumnType.TIMESTAMP_WITH_TIME_ZONE);
        checkSignatureErrorCommute(field1, OBJECT_VAL, type1, SqlColumnType.OBJECT);
    }

    protected void checkFields(Object field1, Object field2, SqlColumnType expectedType, Object expectedResult) {
        put(ExpressionBiValue.createBiValue(field1, field2));

        checkValue0(sql("field1", "field2"), expectedType, expectedResult);
    }

    protected void checkFieldsCommute(Object field1, Object field2, SqlColumnType expectedType, Object expectedResult) {
        put(ExpressionBiValue.createBiValue(field1, field2));

        checkValue0(sql("field1", "field2"), expectedType, expectedResult);
        checkValue0(sql("field2", "field1"), expectedType, expectedResult);
    }

    protected void checkError(Object field1, Object field2, int errorCode, String errorMessage) {
        put(ExpressionBiValue.createBiValue(field1, field2));

        checkFailure0(sql("field1", "field2"), errorCode, errorMessage);
    }

    protected void checkErrorCommute(Object field1, Object field2, int errorCode, String errorMessage) {
        put(ExpressionBiValue.createBiValue(field1, field2));

        checkFailure0(sql("field1", "field2"), errorCode, errorMessage);
        checkFailure0(sql("field2", "field1"), errorCode, errorMessage);
    }

    protected void checkSignatureErrorCommute(Object field1, Object field2, SqlColumnType type1, SqlColumnType type2) {
        put(ExpressionBiValue.createBiValue(field1, field2));

        checkFailure0(sql("field1", "field2"), SqlErrorCode.PARSING, signatureError(type1, type2));
        checkFailure0(sql("field2", "field1"), SqlErrorCode.PARSING, signatureError(type2, type1));
    }

    protected String sql(Object operand1, Object operand2) {
        return "SELECT " + operand1 + " " + operator() + " " + operand2 + " FROM map";
    }

    protected String signatureError(Object type1, Object type2) {
        return signatureErrorOperator(operator(), type1, type2);
    }

    protected static BigDecimal decimal(Object value) {
        return new BigDecimal(value.toString());
    }
}
