/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.util;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for ByteUtil class.
 */
@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class ByteUtilTest {

    private byte b;

    @Before
    public void initByte() {
        b = 0;
    }

    /**
     * Test method for {@link com.hazelcast.util.ByteUtil#setTrue(byte, int)}.
     */
    @Test
    public void testSetTrue() {
        for (int i = 0; i < 8; i++) {
            assertTrue(ByteUtil.isFalse(b, i));
            b = ByteUtil.setTrue(b, i);
            assertTrue(ByteUtil.isTrue(b, i));
        }
    }

    /**
     * Test method for {@link com.hazelcast.util.ByteUtil#setTrue(byte, int)}.
     */
    @Test(expected = ArrayIndexOutOfBoundsException.class)
    public void testSetTrueLowerLimit() {
        ByteUtil.setTrue(b, -1);
    }

    /**
     * Test method for {@link com.hazelcast.util.ByteUtil#setTrue(byte, int)}.
     */
    @Test(expected = ArrayIndexOutOfBoundsException.class)
    public void testSetTrueUpperLimit() {
        ByteUtil.setTrue(b, 8);
    }

    /**
     * Test method for {@link com.hazelcast.util.ByteUtil#setFalse(byte, int)}.
     */
    @Test
    public void testSetFalse() {
        b = ~0;
        for (int i = 0; i < 8; i++) {
            assertTrue(ByteUtil.isTrue(b, i));
            b = ByteUtil.setFalse(b, i);
            assertTrue(ByteUtil.isFalse(b, i));
        }
    }

    /**
     * Test method for {@link com.hazelcast.util.ByteUtil#isTrue(byte, int)}.
     */
    @Test
    public void testIsTrue() {
        for (int i = 0; i < 8; i++) {
            b = ByteUtil.setTrue(b, i);
            assertTrue(ByteUtil.isTrue(b, i));
        }
    }

    /**
     * Test method for {@link com.hazelcast.util.ByteUtil#isFalse(byte, int)}.
     */
    @Test
    public void testIsFalse() {
        for (int i = 0; i < 8; i++) {
            assertTrue(ByteUtil.isFalse(b, i));
        }
    }

    @Test
    public void testToByte() throws Exception {
        assertEquals("00000101", ByteUtil.toBinaryString(ByteUtil.toByte(true, false, true, false)));
        assertEquals("11111111", ByteUtil.toBinaryString(ByteUtil.toByte(true, true, true, true, true, true, true, true)));
        assertEquals("11011011", ByteUtil.toBinaryString(ByteUtil.toByte(true, true, false, true, true, false, true, true)));
        assertEquals("01111111", ByteUtil.toBinaryString(ByteUtil.toByte(true, true, true, true, true, true, true, false)));
    }

    @Ignore
    private void checkFromByte(boolean[] b) {
        final boolean[] fromByte = ByteUtil.fromByte(ByteUtil.toByte(b));
        for (int i = 0; i < b.length; i++) {
            assertEquals(b[i], fromByte[i]);
        }
    }

    @Test
    public void testFromByte() throws Exception {
        checkFromByte(new boolean[]{true, false, true, false});
        checkFromByte(new boolean[]{true, true, true, true, true, true, true, true});
        checkFromByte(new boolean[]{true, true, false, true, true, false, true, true});
        checkFromByte(new boolean[]{true, true, true, true, true, true, true, false});
    }

    @Test
    public void testToBinaryString() throws Exception {
        assertEquals("00000000", ByteUtil.toBinaryString(b));
        assertEquals("00000001", ByteUtil.toBinaryString((byte) 1));
        assertEquals("00000111", ByteUtil.toBinaryString((byte) 7));
        assertEquals("10000000", ByteUtil.toBinaryString((byte) (1 << 7)));
    }
}
