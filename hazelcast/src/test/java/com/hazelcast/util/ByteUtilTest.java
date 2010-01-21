/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.util;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Unit tests for ByteUtil class.
 */
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
}
