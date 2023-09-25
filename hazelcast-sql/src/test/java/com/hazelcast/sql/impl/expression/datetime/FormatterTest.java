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

package com.hazelcast.sql.impl.expression.datetime;

import com.hazelcast.internal.util.JavaVersion;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
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
import java.util.Locale;

import static com.hazelcast.sql.impl.expression.datetime.Formatter.forDates;
import static com.hazelcast.sql.impl.expression.datetime.Formatter.forNumbers;
import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.NaN;
import static java.util.Locale.US;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
@SuppressWarnings("checkstyle:ParenPad")
public class FormatterTest {
    private static final Locale TR = new Locale("tr", "TR");
    private static final int JDK = JavaVersion.CURRENT_VERSION.getMajorVersion();

    @Test
    public void testDates() {
        Formatter f = forDates("FMDay, Mon FMDDth, FMYYYY");
        check(LocalDate.of(2022, 9, 26), f, "Monday, Sep 26th, 2022");

        f = forDates("FMDD FMMonth FMYYYY");
        check(LocalDate.of(2022, 9, 26), f, "26 Eylül 2022", TR);

        f = forDates("YYYY-MM-DD FMHH:MI AM OF");
        check(LocalDateTime.of(2022, 9, 26, 14, 53).atOffset(ZoneOffset.ofHours(3)), f,
                "2022-09-26 2:53 PM +03:00");

        f = forDates("HH12:MI A.M. TZTZH:TZM");
        check(LocalTime.of(14, 53).atOffset(ZoneOffset.ofHours(3)), f, "02:53 P.M. GMT+03:00");

        if (JDK >= 11) {
            f = forDates("HH12:MI A.M.");
            check(LocalTime.of(14, 53), f, "02:53 Ö.S.", TR);
        }

        f = forDates("At HH24:MI:SS, FMSSSS(=FMSSSSS) \"seconds are passed from the midnight.\"");
        check(LocalTime.of(12, 34, 56), f,
                "At 12:34:56, 45296(=45296) seconds are passed from the midnight.");

        f = forDates("YYYY-MM-DD \"is the\" FMDDDth \"day of\" FMYYYY.");
        check(LocalDate.of(2022, 9, 26), f, "2022-09-26 is the 269th day of 2022.");

        f = forDates("Y,YYY YYYY YYY YY Y - FMY,YYY FMYYYY FMYYY FMYY FMY");
        check(Year.of(1), f, "0,001 0001 001 01 1 - 1 1 1 1 1");

        f = forDates("FF1 FF2 FF3(=MS) FF4 FF5 FF6(=US)");
        check(LocalTime.ofNanoOfDay(123456789), f, "1 12 123(=123) 1234 12345 123456(=123456)");

        f = forDates("\"Quarter\" FMYYYY-\"Q\"Q \"is in the\" FMCCth \"century.\"");
        check(YearMonth.of(2022, 9), f, "Quarter 2022-Q3 is in the 21st century.");

        f = forDates("\"Plato founded the Academy in c.\" FMYYYY AD.");
        check(Year.of(-386), f, "Plato founded the Academy in c. 387 BC.");

        f = forDates("\"Plato Akademi'yi\" A.D. FMYYYY \"civarında kurdu.\"");
        check(Year.of(-386), f, "Plato Akademi'yi M.Ö. 387 civarında kurdu.", TR);

        f = forDates("AD(=BC) A.D.(=B.C.) ad(=bc) a.d.(=b.c.)");
        check(Year.of(0), f, "BC(=BC) B.C.(=B.C.) bc(=bc) b.c.(=b.c.)");

        f = forDates("\"The Halley's closest approach to the Earth was on\" FMDD FMMonth FMYYYY AD (the FMJth \"Julian day\").");
        check(LocalDate.of(837, 2, 28), f,
                "The Halley's closest approach to the Earth was on 28 February 837 AD (the 2026827th Julian day).");

        f = forDates("FMRM.FMRD.FMRY");
        check(LocalDate.of(2022, 9, 26), f, "IX.XXVI.MMXXII");
    }

    @Test
    public void testWeekDates() {
        Formatter f = forDates("IYYY-\"W\"IW-FMID");
        check(LocalDate.of(2022, 10, 25), f, "2022-W43-2");
        check(LocalDate.of(2019, 12, 30), f, "2020-W01-1");

        f = forDates("YYYY-MM-DD \"is the\" FMIDDDth \"day of week-year\" FMIYYY.");
        check(LocalDate.of(2008, 12, 29), f, "2008-12-29 is the 1st day of week-year 2009.");
        check(LocalDate.of(2010, 1, 3), f, "2010-01-03 is the 371st day of week-year 2009.");

        f = forDates("IYYY IYY IY I - FMIYYY FMIYY FMIY FMI");
        check(LocalDate.of(1, 1, 1), f, "0001 001 01 1 - 1 1 1 1");
    }

    @Test
    public void testDigitsAndGrouping() {
        Formatter f = forNumbers("00");
        check(-5, f, "-05");

        f = forNumbers(".00");
        check(-0.5, f, "-.50");

        f = forNumbers("0,909.090");
        check(-123.4, f, "-0,123.400");

        f = forNumbers("9,090.909");
        check(-123.4, f, "  -123.40 ");

        f = forNumbers("9G99G999D99");
        check(-123456.78, f, "-1,23,456.78");
    }

    @Test
    public void testRounding() {
        Formatter f = forNumbers("FM0.9");
        check(0.15, f, "0.2");

        f = forNumbers("FM.99");
        check(0.015, f, ".02");

        f = forNumbers("FM99");
        check(9.9, f, "10");
    }

    @Test
    public void testSignAndAnchoring() {
        check(0,  "+0.0",  "-0.0",  "S0.0", "SG0.0");
        check(0,  " +.0",  " -.0",  "S9.0", "9SG.0", "9S.0");
        check(0,  "+0  ",  "-0  ",  "S0.9", "SG0.9");

        check(0,  "0.0+",  "0.0-",  "0.0S", "0.0SG");
        check(0,  " .0+",  " .0-",  "9.0S", "9.0SG");
        check(0,  "0+  ",  "0-  ",  "0.9S", "0SG.9", "0S.9");

        check(0,  "+0  ",  "-0  ",  "SG9.9", "S9.9");
        check(0,  "0+  ",  "0-  ",  "9SG.9", "9S.9", "9.S9", "9.9S");
        check(0,  "0 + ",  "0 - ",  "9.SG9");
        check(0,  "0  +",  "0  -",  "9.9SG");

        check(0,  "+.0",  "-.0",  "SG.9", "S.9");
        check(0,  ".+0",  ".-0",  ".SG9", ".S9");
        check(0,  ".0+",  ".0-",  ".9SG", ".9S");

        check(0,  " 0 ",  "<0>",  "9BR", "BR9", "9B", "B9", "BR9BR", "B9B", "BR9B", "B9BR");

        check(4.5,  "  4.5 ",  "< 4.5>",  "99.9BR", "BR99.9", "BR99.9BR", "BR99.9B");
        check(4.5,  "  4.5 ",  " <4.5>",  "99.9B",   "B99.9",  "B99.9B");
        check(4.5,  " +4.5 ",  " <4.5>",  "P9.9B",   "BP9.9",  "PB9.9");

        check(4.5,  "+ 4.5",  "- 4.5",  "SG99.9");
        check(4.5,  " 4.5+",  " 4.5-",  "99.9SG", "99.9S");
        check(4.5,  " +4.5",  " -4.5",  "S99.9");

        check(4.5,  "  4.5",  "- 4.5",  "MI99.9");
        check(4.5,  " 4.5 ",  " 4.5-",  "99.9MI", "99.9M");
        check(4.5,  "  4.5",  " -4.5",  "M99.9");

        check(4.5,  "+  4.5",  "  -4.5",  "PL99.9");
        check(4.5,  "  4.5+",  " -4.5 ",  "99.9PL", "99.9P");
        check(4.5,  "  +4.5",  "  -4.5",  "P99.9");
    }

    @Test
    public void testMultiplier() {
        check(4.5,  " 450",  "-450",  "999V99", "999V9V9");
        check(0.0485,  " 4.9 E-01",  "-4.9 E-01",  "9.9V9 EEEE", "9V9.9 EEEE", "9.9 V9EEEE", "9.9 EEEEV9");
    }

    @Test
    public void testExponentialForm() {
        check(0,   " 0E+00",   "-0E+00",   "9EEEE");
        check(0,  " .0E+00",  "-.0E+00",  ".9EEEE");

        Formatter f = forNumbers("FM9.99EEEE");
        check(0,         f, "0E+00");
        check(5e4,       f, "5E+04");
        check(0.00045,   f, "4.5E-04");
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

        check(5,  "$ 5 ",  "$<5>",  "CRBR9", "CRB9", "CR9B");
        check(5,  " $5 ",  "$<5>",  "CB9");
        check(5,  " $5 ",  "<$5>",  "BRCR9", "BRC9", "C9BR", "BC9", "C9B");

        check(5,  " 5 $",  "<5>$",  "9BRCR", "9BCR",  "B9CR");
        check(5,  " 5$ ",  "<5>$",  "9BC");
        check(5,  " 5$ ",  "<5$>",  "9CRBR", "9CBR", "BR9C", "9CB", "B9C");
    }

    @Test
    public void testOrdinals() {
        Formatter f = forNumbers("9TH");
        check(-1, f, "-1ST");
        check( 0, f, " 0TH");
        check( 2, f, " 2ND");
        check( 3, f, " 3RD");

        f = forNumbers("FM999th");
        check(410, f, "410th");
        check(411, f, "411th");
        check(412, f, "412th");
        check(413, f, "413th");
        check(421, f, "421st");
        check(422, f, "422nd");
        check(423, f, "423rd");

        check(411.2, f, "411th");
        check(420.5, f, "421st");
    }

    @Test
    public void testRomanNumerals() {
        Formatter f = forNumbers("FMRN");
        check(485, f, "CDLXXXV");

        f = forNumbers("RN");
        check(485,  f, "        CDLXXXV");
        check(3888, f, "MMMDCCCLXXXVIII");
    }

    @Test
    public void testOverflow() {
        Formatter f = forNumbers("9");
        check(-10, f, "-#");
        check(NaN, f, " #");

        f = forNumbers(".9");
        check(-1,  f, "-.#");
        check(NaN, f, " .#");

        f = forNumbers("SG");
        check(-1,  f, "-");
        check(NaN, f, "+");

        f = forNumbers("9th");
        check(-10, f, "-#th");
        check(NaN, f, " #  ");

        f = forNumbers("9.9EEEE");
        check(NEGATIVE_INFINITY, f, "-#.#E+##");
        check(NaN,               f, " #.#E+##");

        f = forNumbers("FMRN");
        check(-1,   f, "###############");
        check(0,    f, "###############");
        check(4000, f, "###############");
        check(NaN,  f, "###############");
    }

    @Test
    public void testLiterals() {
        check(4.5,  "  $4.5",  " -$4.5",   "M$99.9",  "$99.9");
        check(4.5,  "$  4.5",  "$ -4.5",  "F$M99.9", "F$99.9");

        check(4.5,  " 4.5 USD ",  "-4.5 USD ",  "9.99 \"USD\"");
        check(4.5,  " 4.5  USD",  "-4.5  USD",  "9.99 F\"USD\"");

        check(45,  " 4.5",  "-4.5",  "9\".\"9", "9\\.9");
        check(45,  " 450",  "-450",  "99\"0\"", "99\\0");

        Formatter f = forNumbers("FM\"Integer: \"999 \"Fraction: \".999");
        check(48.59, f, "Integer: 48 Fraction: .59");

        f = forNumbers("FM\\\"999\"\\\" + \\\"\".999\\\"");
        check(48.59, f, "\"48\" + \".59\"");
    }

    @Test
    public void testEscaping() {
        check("\"\\\"\"", "\"", "Quote in quotes");
        check("\"\\\"",   "\"", "Quote in unpaired quotes");
        check(  "\\\"",   "\"", "Quote not in quotes");
        check(    "\"",   "",   "Unescaped quote");

        check("\"\\\\\"", "\\", "Backslash in quotes");
        check("\"\\\\",   "\\", "Backslash in unpaired quotes");
        check(  "\\\\",   "\\", "Backslash not in quotes");
        check(    "\\",   "\\", "Unescaped backslash");

        check("\"\\a\"", "a", "Unnecessary escape in quotes");
        check(  "\\a",   "a", "Unnecessary escape not in quotes");
    }

    @Test
    public void testFillMode() {
        check(0,  " 0",  "-0",  "9");
        check(0,   "0",  "-0",  "FM9");

        check(0,  " 0 ",  "<0>",  "B9");
        check(0,   "0",   "<0>",  "FMB9");

        Formatter f = forNumbers("999 (RN)");
        check(0,   f, "   0 ()               ");
        check(988, f, " 988 (CMLXXXVIII)     ");
    }

    @Test
    public void testLocales() {
        Formatter f = forNumbers("FM9G999D99 CR");
        check(1234.56, f, "1,234.56 $", US);
        check(1234.56, f, "1.234,56 " + (JDK >= 11 ? "₺" : "TL"), TR);

        if (JDK >= 17) {
            Locale fr_CH = new Locale("fr", "CH");
            f = forNumbers("FM9G999D99");
            check(1234.56, f, "1 234,56", fr_CH);
            f = forNumbers("FM9G999D99 CR");
            check(1234.56, f, "1 234.56 CHF", fr_CH);
        }
    }

    @Test
    public void testFeatureOrthogonality() {
        Formatter f = forNumbers("FM999th +.99");
        check(421.35, f, "421st +.35");

        f = forNumbers("FM999.99th");
        check(421.35, f, "421.35st");

        f = forNumbers("FM999V99 -> RN");
        check(3.14,  f, "314 -> CCCXIV");
    }

    @Test
    public void testLowercasePatterns() {
        Formatter f = forDates("yyyy-mm-dd fmhh:mi am tzfmtzh");
        check(LocalDateTime.of(2022, 9, 26, 14, 53).atOffset(ZoneOffset.ofHours(3)), f,
                "2022-09-26 2:53 pm gmt+3");

        f = forNumbers("fm9.99eeee");
        check(0.0004859, f, "4.86e-04");

        f = forNumbers("fmrn");
        check(485, f, "cdlxxxv");

        f = forNumbers("9d99 f\"USD\"");
        check(4.5, f, " 4.5  USD");
    }

    @Test
    public void testEmptyOrIncompatibleFormat() {
        check(LocalDate.MIN, forDates(""), "");
        check(0, forNumbers(""), "");

        assertThrows("Input parameter is expected to be date/time", QueryException.class,
                () -> forDates("YYYY-MM-DD").format(0, US));

        assertThrows("Input parameter is expected to be numeric", QueryException.class,
                () -> forNumbers("9").format(LocalDate.MIN, US));
    }

    private void check(String literal, String expected, String message) {
        assertEquals(message, expected, forDates(literal).format(Year.of(0), US));
        assertEquals(message, expected, forNumbers("FM" + literal).format(0, US));
    }

    private void check(double input, String expectedPositive, String expectedNegative, String... formats) {
        for (String format : formats) {
            Formatter f = forNumbers(format);
            check( input, f, expectedPositive);
            check(-input, f, expectedNegative);
        }
    }

    private void check(Object input, Formatter f, String expected) {
        check(input, f, expected, US);
    }

    private void check(Object input, Formatter f, String expected, Locale locale) {
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
