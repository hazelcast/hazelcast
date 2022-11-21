/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.expression.datetime;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Year;
import java.time.YearMonth;
import java.time.ZoneOffset;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
@SuppressWarnings("checkstyle:ParenPad")
public class FormatterTest {
    private static int jdk;

    @BeforeClass
    public static void setup() {
        String[] version = System.getProperty("java.version").split("\\.");
        jdk = Integer.parseInt(version[version[0].equals("1") ? 1 : 0]);
    }

    @Test
    public void testDates() {
        Formatter f = new Formatter("Day, Mon DDth, YYYY");
        check(LocalDate.of(2022, 9, 26), f, "Monday, Sep 26th, 2022");

        f = new Formatter("DD TMMonth YYYY");
        check(LocalDate.of(2022, 9, 26), f, "26 Eylül 2022", "tr-TR");

        f = new Formatter("YYYY-FMMM-FMDD HH:FMMI AM OF");
        check(LocalDateTime.of(2022, 9, 26, 14, 53).atOffset(ZoneOffset.ofHours(3)), f,
                "2022-09-26 2:53 PM +03:00");

        f = new Formatter("FMHH12:FMMI A.M. TZFMTZH:FMTZM");
        check(LocalTime.of(14, 53).atOffset(ZoneOffset.ofHours(3)), f, "02:53 P.M. GMT+03:00");

        if (jdk >= 11) {
            f = new Formatter("FMHH12:FMMI TMA.M.");
            check(LocalTime.of(14, 53), f, "02:53 Ö.S.", "tr-TR");
        }

        f = new Formatter("At HH24:MI:SS, SSSS(=SSSSS) seconds are passed from the midnight.");
        check(LocalTime.of(12, 34, 56), f,
                "At 12:34:56, 45296(=45296) seconds are passed from the midnight.");

        f = new Formatter("YYYY-MM-DD is the DDDth \"day\" of YYYY.");
        check(LocalDate.of(2022, 9, 26), f, "2022-9-26 is the 269th day of 2022.");

        f = new Formatter("FMY,YYY(Y,YYY) FMYYYY(YYYY) FMYYY(YYY) FMYY(YY) FMY(Y)");
        check(Year.of(1), f, "0,001(1) 0001(1) 001(1) 01(1) 1(1)");

        f = new Formatter("FF1 FF2 FF3(=MS) FF4 FF5 FF6(=US)");
        check(LocalTime.ofNanoOfDay(123456789), f, "1 12 123(=123) 1234 12345 123456(=123456)");

        f = new Formatter("\"Quarter\" YYYY-\"Q\"Q is in the CCth century.");
        check(YearMonth.of(2022, 9), f, "Quarter 2022-Q3 is in the 21st century.");

        f = new Formatter("Roman numeral for Month is RM.");
        check(YearMonth.of(2022, 9), f, "Roman numeral for September is IX.");

        f = new Formatter("Plato founded the \"Academy\" in c. YYYY AD.");
        check(Year.of(-386), f, "Plato founded the Academy in c. 387 BC.");

        f = new Formatter("Plato \"Akademi\"'yi TMA.D. YYYY civarında kurdu.");
        check(Year.of(-386), f, "Plato Akademi'yi M.Ö. 387 civarında kurdu.", "tr-TR");

        f = new Formatter("AD(=BC) A.D.(=B.C.) ad(=bc) a.d.(=b.c.)");
        check(Year.of(0), f, "BC(=BC) B.C.(=B.C.) bc(=bc) b.c.(=b.c.)");

        f = new Formatter("The Halley's closest approach to the Earth was on DD Month YYYY AD (the Jth \"Julian day\").");
        check(LocalDate.of(837, 2, 28), f,
                "The Halley's closest approach to the Earth was on 28 February 837 AD (the 2026827th Julian day).");
    }

    @Test
    public void testWeekDates() {
        Formatter f = new Formatter("IYYY-\"W\"FMIW-ID");
        check(LocalDate.of(2022, 10, 25), f, "2022-W43-2");
        check(LocalDate.of(2019, 12, 30), f, "2020-W01-1");

        f = new Formatter("YYYY-FMMM-FMDD is the IDDDth \"day\" of week-year IYYY.");
        check(LocalDate.of(2008, 12, 29), f, "2008-12-29 is the 1st day of week-year 2009.");
        check(LocalDate.of(2010, 1, 3), f, "2010-01-03 is the 371st day of week-year 2009.");

        f = new Formatter("FMIYYY(IYYY) FMIYY(IYY) FMIY(IY) FMI(I)");
        check(LocalDate.of(1, 1, 1), f, "0001(1) 001(1) 01(1) 1(1)");
    }

    @Test
    public void testRounding() {
        Formatter f = new Formatter("FM0.9");
        check(0.15, f, "0.2");
    }

    @Test
    public void testGrouping() {
        Formatter f = new Formatter("9G99G999D990");
        check(-123456.78, f, "-1,23,456.780");
    }

    @Test
    public void testSign() {
        Formatter f = new Formatter("0.9S");
        check( 1, f, "1+  ");
        check( 0, f, "0+  ");
        check(-1, f, "1-  ");

        check(4.5,  "  4.5 ",  "< 4.5>",  "99.9BR", "BR99.9", "BR99.9BR", "BR99.9B");
        check(4.5,  "  4.5 ",  " <4.5>",  "99.9B",   "B99.9",  "B99.9B");
        check(4.5,  " +4.5 ",  " <4.5>",  "P9.9B",   "BP9.9",  "PB9.9");

        check(4.5,  "+ 4.5",  "- 4.5",  "SG99.9");
        check(4.5,  " 4.5+",  " 4.5-",  "99.9SG", "99.9S");
        check(4.5,  " +4.5",  " -4.5",  "S99.9");

        check(4.5,  "  4.5",  "- 4.5",  "MI99.9");
        check(4.5,  " 4.5 ",  " 4.5-",  "99.9MI", "99.9M");
        check(4.5,  "  4.5",  " -4.5",  "M99.9");

        check(4.5,  "+ 4.5",  "  4.5",  "PL99.9");
        check(4.5,  " 4.5+",  " 4.5 ",  "99.9PL", "99.9P");
        check(4.5,  " +4.5",  "  4.5",  "P99.9");
    }

    @Test
    public void testMultiplier() {
        check(4.85,  " 485",  "-485",  "999V99", "999V9V9", "V9FMV9FM999");
        check(0.0485,  " 4.9 E-01",  "-4.9 E-01",  "9.9V9 EEEE", "9V9.9 EEEE", "9.9 V9EEEE", "9.9 EEEEV9");
    }

    @Test
    public void testExponentialForm() {
        Formatter f = new Formatter("FM9.99EEEE");
        check(0.0004859, f, "4.86E-04");
    }

    @Test
    public void testSignAndCurrencyAnchoring() {
        check(4.5,  "$  4.5",  "$- 4.5",  "CRMI99.9");
        check(4.5,  "$  4.5",  "$ -4.5",   "CRM99.9", "CR99.9");
        check(4.5,  "  $4.5",  " $-4.5",    "CM99.9");
        check(4.5,  " $ 4.5",  "-$ 4.5",  "MICR99.9");
        check(4.5,  "  $4.5",  "- $4.5",   "MIC99.9");
        check(4.5,  "  $4.5",  " -$4.5",    "MC99.9",  "C99.9");

        check(4.5,  "$  4.5 ",  "$< 4.5>",  "CRBR99.9", "CR99.9BR");
        check(4.5,  "$  4.5 ",  "$ <4.5>",   "CRB99.9", "CR99.9B");
        check(4.5,  "  $4.5 ",  " $<4.5>",    "CB99.9");
        check(4.5,  " $ 4.5 ",  "<$ 4.5>",  "BRCR99.9");
        check(4.5,  "  $4.5 ",  "< $4.5>",   "BRC99.9",  "C99.9BR");
        check(4.5,  "  $4.5 ",  " <$4.5>",    "BC99.9",  "C99.9B");
    }

    @Test
    public void testOrdinals() {
        Formatter f = new Formatter("FM999th");
        check(410, f, "410th");
        check(411, f, "411th");
        check(412, f, "412th");
        check(413, f, "413th");
        check(421, f, "421st");
        check(422, f, "422nd");
        check(423, f, "423rd");
    }

    @Test
    public void testRomanNumerals() {
        Formatter f = new Formatter("FMRN");
        check(485, f, "CDLXXXV");

        f = new Formatter("RN");
        check(485,  f, "        CDLXXXV");
        check(3888, f, "MMMDCCCLXXXVIII");
        check(0,    f, "###############");
        check(4000, f, "###############");
    }

    @Test
    public void testLiterals() {
        Formatter f = new Formatter("FM\"Integer: \"999 \"Fraction: \".999");
        check(48.59, f, "Integer: 48 Fraction: .59");

        check(4.5,  "  $4.5",  " -$4.5",   "M$99.9",  "$99.9");
        check(4.5,  "$  4.5",  "$ -4.5",  "F$M99.9", "F$99.9");

        check(4.5,  " 4.5 USD ",  "-4.5 USD ",  "9.99 \"USD\"");
        check(4.5,  " 4.5  USD",  "-4.5  USD",  "9.99 F\"USD\"");
    }

    @Test
    public void testFeatureOrthogonality() {
        Formatter f = new Formatter("FM999V99 -> RN");
        check(3.14,  f, "314 -> CCCXIV");
    }

    @Test
    public void testLocales() {
        Formatter f = new Formatter("FM9G999D99");
        check(1234.56, f, "1,234.56", "en-US");
        check(1234.56, f, "1.234,56", "tr-TR");
    }

    private void check(double input, String expectedPositive, String expectedNegative, String... formats) {
        for (String format : formats) {
            Formatter f = new Formatter(format);
            check( input, f, expectedPositive);
            check(-input, f, expectedNegative);
        }
    }

    private void check(Object input, Formatter f, String expected) {
        check(input, f, expected, "en-US");
    }

    private void check(Object input, Formatter f, String expected, String locale) {
        assertEquals(expected, f.format(input, locale));
    }

    /**
     * {@link DecimalFormat} makes rounding on the binary representation of
     * numbers, which produces wrong results even for simple cases.
     */
    @Test
    public void testDecimalFormat() {
        DecimalFormat df = new DecimalFormat("#.#");
        df.setRoundingMode(RoundingMode.HALF_UP);
        assertNotEquals("0.2", df.format(0.15));
    }
}
