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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class FormatterTest {
    @Test
    public void testNumberFormat() {
        Formatter f = new Formatter("FM0.9");
        assertEquals("0.2", f.format(0.15));

        f = new Formatter("0.9S");
        assertEquals("0.1-", f.format(-0.1));
        assertEquals("0+  ", f.format(0));

        f = new Formatter("FM9.99EEEE");
        assertEquals("4.86E-04", f.format(0.0004859));

        f = new Formatter("FM999th");
        assertEquals("410th", f.format(410));
        assertEquals("411th", f.format(411));
        assertEquals("412th", f.format(412));
        assertEquals("413th", f.format(413));
        assertEquals("421st", f.format(421));
        assertEquals("422nd", f.format(422));
        assertEquals("423rd", f.format(423));

        f = new Formatter("FMRN");
        assertEquals("CDLXXXV", f.format(485));

        f = new Formatter("9.99PR");
        assertEquals("<4.85>", f.format(-4.85));

        f = new Formatter("S99.9MI");
        assertEquals("-48  -", f.format(-48));

        f = new Formatter("S999V99");
        assertEquals("+485", f.format(4.85));

        f = new Formatter("9G99G999D990");
        assertEquals("-1,23,456.780", f.format(-123456.78));
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
