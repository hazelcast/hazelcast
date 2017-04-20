/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.util;

import org.junit.Test;

import static com.hazelcast.jet.impl.util.Util.addClamped;
import static com.hazelcast.jet.impl.util.Util.subtractClamped;
import static org.junit.Assert.assertEquals;

public class UtilTest {

    @Test
    public void testAddClamped() {
        // no overflow
        assertEquals(0, addClamped(0, 0));
        assertEquals(1, addClamped(1, 0));
        assertEquals(-1, addClamped(-1, 0));
        assertEquals(-1, addClamped(Long.MAX_VALUE, Long.MIN_VALUE));

        // overflow over MAX_VALUE
        assertEquals(Long.MAX_VALUE, addClamped(Long.MAX_VALUE, 1));
        assertEquals(Long.MAX_VALUE, addClamped(Long.MAX_VALUE, Long.MAX_VALUE));

        // overflow over MIN_VALUE
        assertEquals(Long.MIN_VALUE, addClamped(Long.MIN_VALUE, -1));
        assertEquals(Long.MIN_VALUE, addClamped(Long.MIN_VALUE, Long.MIN_VALUE));
    }

    @Test
    public void testSubtractClamped() {
        // no overflow
        assertEquals(0, subtractClamped(0, 0));
        assertEquals(1, subtractClamped(1, 0));
        assertEquals(-1, subtractClamped(-1, 0));
        assertEquals(0, subtractClamped(Long.MAX_VALUE, Long.MAX_VALUE));

        // overflow over MAX_VALUE
        assertEquals(Long.MAX_VALUE, subtractClamped(Long.MAX_VALUE, -1));
        assertEquals(Long.MAX_VALUE, subtractClamped(Long.MAX_VALUE, Long.MIN_VALUE));

        // overflow over MIN_VALUE
        assertEquals(Long.MIN_VALUE, subtractClamped(Long.MIN_VALUE, 1));
        assertEquals(Long.MIN_VALUE, subtractClamped(Long.MIN_VALUE, Long.MAX_VALUE));
    }

}
