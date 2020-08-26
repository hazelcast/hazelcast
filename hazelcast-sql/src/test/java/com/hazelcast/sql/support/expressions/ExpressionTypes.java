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

package com.hazelcast.sql.support.expressions;

public final class ExpressionTypes {

    public static final ExpressionType<?> BOOLEAN = new ExpressionType.BooleanType();
    public static final ExpressionType<?> BYTE = new ExpressionType.ByteType();
    public static final ExpressionType<?> SHORT = new ExpressionType.ShortType();
    public static final ExpressionType<?> INTEGER = new ExpressionType.IntegerType();
    public static final ExpressionType<?> LONG = new ExpressionType.LongType();
    public static final ExpressionType<?> BIG_DECIMAL = new ExpressionType.BigDecimalType();
    public static final ExpressionType<?> BIG_INTEGER = new ExpressionType.BigIntegerType();
    public static final ExpressionType<?> FLOAT = new ExpressionType.FloatType();
    public static final ExpressionType<?> DOUBLE = new ExpressionType.DoubleType();
    public static final ExpressionType<?> STRING = new ExpressionType.StringType();
    public static final ExpressionType<?> CHARACTER = new ExpressionType.CharacterType();
    public static final ExpressionType<?> OBJECT = new ExpressionType.ObjectType();
    public static final ExpressionType<?> LOCAL_DATE = new ExpressionType.LocalDateType();
    public static final ExpressionType<?> LOCAL_TIME = new ExpressionType.LocalTimeType();
    public static final ExpressionType<?> LOCAL_DATE_TIME = new ExpressionType.LocalDateTimeType();
    public static final ExpressionType<?> OFFSET_DATE_TIME = new ExpressionType.OffsetDateTimeType();

    private ExpressionTypes() {
        // No-op.
    }
}
