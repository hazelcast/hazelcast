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
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.Locale;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class FormatterTest {
    static {
        Locale.setDefault(Locale.US);
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
    public void testSigns() {
        Formatter f = new Formatter("0.9S");
        check(-0.1, f, "0.1-");
        check( 0,   f, "0+  ");

        f = new Formatter("9.99MI");
        check(-4.85, f, "4.85-");
        check( 4.85, f, "4.85 ");

        f = new Formatter("9.99PL");
        check(-4.85, f, "4.85 ");
        check( 4.85, f, "4.85+");

        f = new Formatter("9.99SG");
        check(-4.85, f, "4.85-");
        check( 4.85, f, "4.85+");

        f = new Formatter("9.99PR");
        check(-4.85, f, "<4.85>");
        check( 4.85, f, " 4.85 ");
    }

    @Test
    public void testMultiplier() {
        Formatter f = new Formatter("999V99");
        check(4.85, f, " 485");
    }

    @Test
    public void testExponentialForm() {
        Formatter f = new Formatter("FM9.99EEEE");
        check(0.0004859, f, "4.86E-04");
    }

    @Test
    public void testCurrency() {
        Formatter f = new Formatter("9.99L");
        check(-4.85, f, "-4.85$");
        check( 4.85, f, " 4.85$");

        f = new Formatter("L9.99");
        check(-4.85, f, "$-4.85");
        check( 4.85, f, "$ 4.85");
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
    }

//    private void check(Object input, String format, String expected) {
//        check(input, new Formatter(format), expected);
//    }
    private void check(Object input, Formatter f, String expected) {
        assertEquals(expected, f.format(input));
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
