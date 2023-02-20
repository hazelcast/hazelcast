/*
 * Copyright 2023 Hazelcast Inc.
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

package com.hazelcast.jet.sql.impl.expression.datetime;

import com.hazelcast.jet.sql.impl.expression.ExpressionTestSupport;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.Temporal;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.sql.SqlColumnType.VARCHAR;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ToCharIntegrationTest extends ExpressionTestSupport {
    private static Map<Class<? extends Temporal>, DateTimeFormatter> literals;

    @BeforeClass
    public static void setup() {
        literals = new HashMap<>();
        literals.put(LocalDate.class, DateTimeFormatter.ofPattern("'DATE' ''yyyy-MM-dd''"));
        literals.put(LocalTime.class, DateTimeFormatter.ofPattern("'TIME' ''HH:mm:ss''"));
        literals.put(LocalDateTime.class, DateTimeFormatter.ofPattern("'TIMESTAMP' ''yyyy-MM-dd HH:mm:ss''"));
        literals.put(OffsetDateTime.class,
                DateTimeFormatter.ofPattern("'TIMESTAMP WITH TIME ZONE' ''yyyy-MM-dd HH:mm:ssx''"));
    }

    @Test
    public void testTemporals() {
        check(LocalDate.of(2022, 9, 26), "FMDay, Mon FMDDth, FMYYYY", "Monday, Sep 26th, 2022");
        check(LocalDate.of(2022, 9, 26), "FMDD FMMonth FMYYYY", "tr-TR", "26 Eylül 2022");
        check(LocalTime.of(14, 53, 34), "FMHH:MI:SS AM", "2:53:34 PM");
        check(LocalDateTime.of(2022, 9, 26, 14, 53), "FMDay, Mon FMDDth, HH24:MI", "Monday, Sep 26th, 14:53");
//        check(LocalDateTime.of(2022, 9, 26, 14, 53).atOffset(ZoneOffset.ofHours(3)),
//                "FMDay, Mon FMDDth, HH24:MI UTCFMTZH", "Monday, Sep 26th, 14:53 UTC+03");
    }

    @Test
    public void testNumbers() {
        check(Byte.MIN_VALUE, "999", "-128");
        check(Short.MIN_VALUE, "99999", "-32768");
        check(Integer.MIN_VALUE, "9999999999", "-2147483648");
        check(Long.MIN_VALUE, "9999999999999999999", "-9223372036854775808");
        check(-Float.MAX_VALUE, "9.9999999EEEE", "-3.4028235E+38");
        check(-Double.MAX_VALUE, "9.9999999999999999EEEE", "-1.7976931348623157E+308");
        check(-3.4e+38f, "9.9EEEE", "-3.4E+38");
        check(-3.5e+38d, "9.9EEEE", "-3.5E+38");
        check(new BigInteger("-9223372036854775809"), "9999999999999999999", "-9223372036854775809");
        check(new BigDecimal("-1.23456789012345678901234567890123456789012345678901234"),
                "9.99999999999999999999999999999999999999999999999999999",
                "-1.23456789012345678901234567890123456789012345678901234");
        check(new BigDecimal("-3.4e+38"), "9.9EEEE", "-3.4E+38");
        check(new BigDecimal("-3.5e+38"), "9.9EEEE", "-3.5E+38");
//        check(new BigDecimal("-1.8e+308"), "9.9EEEE", "-1.8E+308");
    }

    private void check(Object input, String format, String result) {
        check(input, format, null, result);
    }

    private void check(Object input, String format, String locale, String result) {
        String rest = "'" + format + "'" + (locale == null ? "" : ", '" + locale + "'");
        putAndCheckValue(input, "SELECT TO_CHAR(this, " + rest + ") FROM map", VARCHAR, result);
        input = input instanceof Temporal
                ? literals.get(input.getClass()).format((Temporal) input)
                : input.toString();
        checkValue0("SELECT TO_CHAR(" + input + ", " + rest + ")", VARCHAR, result);
    }
}
