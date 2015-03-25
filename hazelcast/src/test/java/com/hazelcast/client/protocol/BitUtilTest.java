/*
 * Copyright 2014 Real Logic Ltd.
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
package com.hazelcast.client.protocol;

import com.hazelcast.client.impl.protocol.util.BitUtil;
import org.junit.Test;

import static java.lang.Integer.valueOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;
import static com.hazelcast.client.impl.protocol.util.BitUtil.*;

public class BitUtilTest
{
    @Test
    public void shouldReturnNextPositivePowerOfTwo()
    {
        assertThat(valueOf(findNextPositivePowerOfTwo(Integer.MIN_VALUE)), is(valueOf(Integer.MIN_VALUE)));
        assertThat(valueOf(findNextPositivePowerOfTwo(Integer.MIN_VALUE + 1)), is(valueOf(1)));
        assertThat(valueOf(findNextPositivePowerOfTwo(-1)), is(valueOf(1)));
        assertThat(valueOf(findNextPositivePowerOfTwo(0)), is(valueOf(1)));
        assertThat(valueOf(findNextPositivePowerOfTwo(1)), is(valueOf(1)));
        assertThat(valueOf(findNextPositivePowerOfTwo(2)), is(valueOf(2)));
        assertThat(valueOf(findNextPositivePowerOfTwo(3)), is(valueOf(4)));
        assertThat(valueOf(findNextPositivePowerOfTwo(4)), is(valueOf(4)));
        assertThat(valueOf(findNextPositivePowerOfTwo(31)), is(valueOf(32)));
        assertThat(valueOf(findNextPositivePowerOfTwo(32)), is(valueOf(32)));
        assertThat(valueOf(findNextPositivePowerOfTwo(1 << 30)), is(valueOf(1 << 30)));
        assertThat(valueOf(findNextPositivePowerOfTwo((1 << 30) + 1)), is(valueOf(Integer.MIN_VALUE)));
    }

    @Test
    public void shouldAlignValueToNextMultipleOfAlignment()
    {
        final int alignment = BitUtil.CACHE_LINE_LENGTH;

        assertThat(valueOf(align(0, alignment)), is(valueOf(0)));
        assertThat(valueOf(align(1, alignment)), is(valueOf(alignment)));
        assertThat(valueOf(align(alignment, alignment)), is(valueOf(alignment)));
        assertThat(valueOf(align(alignment + 1, alignment)), is(valueOf(alignment * 2)));

        final int reminder = Integer.MAX_VALUE % alignment;
        final int maxMultiple = Integer.MAX_VALUE - reminder;

        assertThat(valueOf(align(maxMultiple, alignment)), is(valueOf(maxMultiple)));
        assertThat(valueOf(align(Integer.MAX_VALUE, alignment)), is(valueOf(Integer.MIN_VALUE)));
    }

    @Test
    public void shouldDetectEvenAndOddNumbers()
    {
        assertTrue(BitUtil.isEven(0));
        assertTrue(BitUtil.isEven(2));
        assertTrue(BitUtil.isEven(Integer.MIN_VALUE));

        assertFalse(BitUtil.isEven(1));
        assertFalse(BitUtil.isEven(-1));
        assertFalse(BitUtil.isEven(Integer.MAX_VALUE));
    }
}
