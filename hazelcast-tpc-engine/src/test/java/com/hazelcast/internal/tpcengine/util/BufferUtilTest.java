/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.tpcengine.util;

import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;

public class BufferUtilTest {
    @Test
    public void test_put_exactlyEnoughSpace() {
        ByteBuffer src = ByteBuffer.allocate(8);
        src.putInt(1);
        src.putInt(2);
        src.flip();
        int srcPos = src.position();
        int srcLimit = src.limit();

        ByteBuffer dst = ByteBuffer.allocate(8);
        BufferUtil.put(dst, src);
        dst.flip();
        assertEquals(8, dst.remaining());
        assertEquals(1, dst.getInt());
        assertEquals(2, dst.getInt());

        assertEquals(srcPos + 8, src.position());
        assertEquals(srcLimit, src.limit());
    }

    @Test
    public void test_put_moreThanEnoughSpace() {
        ByteBuffer src = ByteBuffer.allocate(8);
        src.putInt(1);
        src.putInt(2);
        src.flip();
        int srcPos = src.position();
        int srcLimit = src.limit();

        ByteBuffer dst = ByteBuffer.allocate(12);
        BufferUtil.put(dst, src);
        dst.flip();
        assertEquals(8, dst.remaining());
        assertEquals(1, dst.getInt());
        assertEquals(2, dst.getInt());
        assertEquals(srcPos + 8, src.position());
        assertEquals(srcLimit, src.limit());
    }

    @Test
    public void test_put_notEnoughSpace() {
        ByteBuffer src = ByteBuffer.allocate(8);
        src.putInt(1);
        src.putInt(2);
        src.flip();
        int srcPos = src.position();
        int srcLimit = src.limit();

        ByteBuffer dst = ByteBuffer.allocate(4);
        BufferUtil.put(dst, src);
        dst.flip();
        assertEquals(4, dst.remaining());
        assertEquals(1, dst.getInt());

        assertEquals(srcPos + 4, src.position());
        assertEquals(srcLimit, src.limit());
    }
}
