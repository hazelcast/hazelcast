/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.sql.impl.expression.math;

import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.sql.impl.SqlErrorCode;
import com.hazelcast.sql.impl.expression.ExpressionTestSupport;
import com.hazelcast.sql.support.expressions.ExpressionBiValue;
import com.hazelcast.sql.support.expressions.ExpressionType;

import java.math.BigDecimal;

public abstract class ArithmeticOperatorIntegrationTest extends ExpressionTestSupport {

    protected abstract String operator();

    protected void checkUnsupportedCommute(Object field1, SqlColumnType type1, ExpressionType<?>... unsupportedTypes) {
        for (ExpressionType<?> unsupportedType : unsupportedTypes) {
            checkSignatureErrorCommute(
                field1,
                unsupportedType.valueFrom(),
                type1,
                unsupportedType.getFieldConverterType().getTypeFamily().getPublicType()
            );
        }
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

    protected String signatureError(SqlColumnType type1, SqlColumnType type2) {
        return signatureErrorOperator( operator(), type1, type2);
    }

    protected static BigDecimal decimal(Object value) {
        return new BigDecimal(value.toString());
    }
}
