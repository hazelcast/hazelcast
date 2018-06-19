/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.impl.protocol.util.MessageFlyweight;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.nio.ByteBuffer;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Flyweight Tests
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class FlyweightTest {

    private static final byte[] DATA = new byte[]{
            (byte) 0x61, (byte) 0x62, (byte) 0x63, (byte) 0xC2, (byte) 0xA9, (byte) 0xE2, (byte) 0x98, (byte) 0xBA,
    };

    private MessageFlyweight flyweight = new MessageFlyweight();
    private ByteBuffer byteBuffer;

    @Before
    public void setUp() {
        byteBuffer = ByteBuffer.allocate(512);

        flyweight.wrap(byteBuffer.array(), 0, true);
    }

    @After
    public void tearDown() {
    }

    @Test
    public void shouldEncodeLong() {
        flyweight.set(0x12345678L);
        assertEquals(8, flyweight.index());
        assertThat(byteBuffer.get(0), is((byte) 0x78));
        assertThat(byteBuffer.get(1), is((byte) 0x56));
        assertThat(byteBuffer.get(2), is((byte) 0x34));
        assertThat(byteBuffer.get(3), is((byte) 0x12));
    }

    @Test
    public void shouldEncodeInt() {
        flyweight.set(0x1234);
        assertEquals(4, flyweight.index());
        assertThat(byteBuffer.get(0), is((byte) 0x34));
        assertThat(byteBuffer.get(1), is((byte) 0x12));
    }

    @Test
    public void shouldEncodeBoolean() {
        flyweight.set(true);
        assertEquals(1, flyweight.index());
        assertThat(byteBuffer.get(0), is((byte) 0x1));
    }

    @Test
    public void shouldEncodeStringUtf8() {
        //0x61 0x62 0x63 0xC2 0xA9 0xE2 0x98 0xBA
        flyweight.set("abc©☺");
        assertEquals(12, flyweight.index());
        assertThat(byteBuffer.get(0), is((byte) 0x08));
        assertThat(byteBuffer.get(1), is((byte) 0x00));
        assertThat(byteBuffer.get(2), is((byte) 0x00));
        assertThat(byteBuffer.get(3), is((byte) 0x00));
        assertThat(byteBuffer.get(4), is((byte) 0x61));
        assertThat(byteBuffer.get(5), is((byte) 0x62));
        assertThat(byteBuffer.get(6), is((byte) 0x63));
        assertThat(byteBuffer.get(7), is((byte) 0xC2));
        assertThat(byteBuffer.get(8), is((byte) 0xA9));
        assertThat(byteBuffer.get(9), is((byte) 0xE2));
        assertThat(byteBuffer.get(10), is((byte) 0x98));
        assertThat(byteBuffer.get(11), is((byte) 0xBA));
    }

    @Test
    public void shouldEncodeByteArray() {
        byte[] data = new byte[]{
                (byte) 0x61, (byte) 0x62, (byte) 0x63, (byte) 0xC2, (byte) 0xA9, (byte) 0xE2, (byte) 0x98, (byte) 0xBA,
        };

        flyweight.set(data);
        assertEquals(12, flyweight.index());
        assertThat(byteBuffer.get(0), is((byte) 0x08));
        assertThat(byteBuffer.get(1), is((byte) 0x00));
        assertThat(byteBuffer.get(2), is((byte) 0x00));
        assertThat(byteBuffer.get(3), is((byte) 0x00));
        assertThat(byteBuffer.get(4), is((byte) 0x61));
        assertThat(byteBuffer.get(5), is((byte) 0x62));
        assertThat(byteBuffer.get(6), is((byte) 0x63));
        assertThat(byteBuffer.get(7), is((byte) 0xC2));
        assertThat(byteBuffer.get(8), is((byte) 0xA9));
        assertThat(byteBuffer.get(9), is((byte) 0xE2));
        assertThat(byteBuffer.get(10), is((byte) 0x98));
        assertThat(byteBuffer.get(11), is((byte) 0xBA));
    }

    @Test
    public void shouldDecodeLong() {
        flyweight.set(0x12345678L);
        flyweight.index(0);
        assertEquals(0x12345678L, flyweight.getLong());
    }

    @Test
    public void shouldDecodeInt() {
        flyweight.set(0x1234);
        flyweight.index(0);
        assertEquals(0x1234, flyweight.getInt());
        assertEquals(4, flyweight.index());
    }

    @Test
    public void shouldDecodeBoolean() {
        flyweight.set(true);
        flyweight.index(0);
        assertTrue(flyweight.getBoolean());
        assertEquals(1, flyweight.index());
    }

    @Test
    public void shouldDecodeStringUtf8() {
        final String staticValue = "abc©☺";
        flyweight.set(staticValue);
        flyweight.index(0);

        assertThat(flyweight.getStringUtf8(), is(staticValue));
        assertEquals(12, flyweight.index());
    }

    @Test
    public void shouldDecodeByteArray() {
        flyweight.set(DATA);
        flyweight.index(0);

        assertArrayEquals(DATA, flyweight.getByteArray());
        assertEquals(4 + DATA.length, flyweight.index());
    }

    @Test
    public void shouldDecodeData() {
        flyweight.set(DATA);
        flyweight.index(0);

        assertArrayEquals(DATA, flyweight.getData().toByteArray());
        assertEquals(4 + DATA.length, flyweight.index());
    }

    @Test
    public void shouldEncodeDecodeMultipleData() {
        flyweight.set(0x12345678L);
        flyweight.set(0x1234);
        flyweight.set((short) 0x12);
        flyweight.set(true);
        flyweight.set(DATA);
    }
}
