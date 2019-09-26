/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.expression;

/**
 * Function operator.
 */
public final class CallOperator {
    /** Plus function: A + B */
    public static final int PLUS = 0;

    /** Minus function: A - B */
    public static final int MINUS = 1;

    /** Minus function: A * B */
    public static final int MULTIPLY = 3;

    /** Divide function: A / B */
    public static final int DIVIDE = 4;

    /** Divide function: -A */
    public static final int UNARY_MINUS = 5;

    /** Type: COS. */
    public static final int COS = 10;

    /** Type: SIN. */
    public static final int SIN = 11;

    /** Type: TAN. */
    public static final int TAN = 12;

    /** Type: COT. */
    public static final int COT = 13;

    /** Type: ACOS. */
    public static final int ACOS = 14;

    /** Type: ASIN. */
    public static final int ASIN = 15;

    /** Type: ATAN. */
    public static final int ATAN = 16;

    /** Type: SQRT. */
    public static final int SQRT = 17;

    /** Type: EXP. */
    public static final int EXP = 18;

    /** Type: LN. */
    public static final int LN = 19;

    /** Type: LOG10. */
    public static final int LOG10 = 20;

    /** Type: RAND. */
    public static final int RAND = 21;

    /** Type: ABS. */
    public static final int ABS = 22;

    /** Type: PI. */
    public static final int PI = 23;

    /** Type: SIGN. */
    public static final int SIGN = 24;

    /** Type: ATAN2. */
    public static final int ATAN2 = 25;

    /** Type: DEGREES. */
    public static final int DEGREES = 26;

    /** Type: RADIANS. */
    public static final int RADIANS = 27;

    /** Type: POWER. */
    public static final int POWER = 28;

    /** Type: FLOOR. */
    public static final int FLOOR = 29;

    /** Type: CEIL. */
    public static final int CEIL = 30;

    /** Divide function: A % B  or MOD(A, B). */
    public static final int REMAINDER = 31;

    /** Type: ROUND. */
    public static final int ROUND = 32;

    /** Type: TRUNCATE. */
    public static final int TRUNCATE = 33;

    /** Character length. */
    public static final int CHAR_LENGTH = 101;

    /** UPPER function. */
    public static final int UPPER = 102;

    /** LOWER function. */
    public static final int LOWER = 103;

    /** INITCAP function. */
    public static final int INITCAP = 104;

    /** ASCII function. */
    public static final int ASCII = 105;

    /** CONCAT function. */
    public static final int CONCAT = 106;

    /** POSITION function. */
    public static final int POSITION = 107;

    /** REPLACE function. */
    public static final int REPLACE = 108;

    /** EXTRACT function. */
    public static final int EXTRACT = 201;

    /** CURRENT_DATE function. */
    public static final int CURRENT_DATE = 202;

    /** CURRENT_TIMESTAMP function. */
    public static final int CURRENT_TIMESTAMP = 203;

    /** LOCAL_TIMESTAMP function. */
    public static final int LOCAL_TIMESTAMP = 204;

    /** LOCAL_TIME function. */
    public static final int LOCAL_TIME = 205;

    /** TIMESTAMPADD function. */
    public static final int TIMESTAMP_ADD = 206;

    /** IS NULL operator. */
    public static final int IS_NULL = 301;

    /** IS NOT NULL operator. */
    public static final int IS_NOT_NULL = 302;

    /** IS FALSE operator. */
    public static final int IS_FALSE = 303;

    /** IS NOT FALSE operator. */
    public static final int IS_NOT_FALSE = 304;

    /** IS TRUE operator. */
    public static final int IS_TRUE = 305;

    /** IS NOT TRUE operator. */
    public static final int IS_NOT_TRUE = 306;

    /** AND operator. */
    public static final int AND = 307;

    /** OR operator. */
    public static final int OR = 308;

    /** "=" operator. */
    public static final int EQUALS = 309;

    /** "<>" operator. */
    public static final int NOT_EQUALS = 310;

    /** ">" operator. */
    public static final int GREATER_THAN = 311;

    /** ">=" operator. */
    public static final int GREATER_THAN_EQUAL = 312;

    /** "<" operator. */
    public static final int LESS_THAN = 313;

    /** "<=" operator. */
    public static final int LESS_THAN_EQUAL = 314;

    /** CASE ... WHEN expression. */
    public static final int CASE = 315;

    /** Item operator. */
    public static final int ITEM = 401;

    /** Aggregate: SUM. */
    public static final int AGG_SUM = 501;

    /** Aggregate: COUNT. */
    public static final int AGG_COUNT = 502;

    /** Aggregate: AVG. */
    public static final int AGG_AVG = 503;

    private CallOperator() {
        // No-op.
    }
}
