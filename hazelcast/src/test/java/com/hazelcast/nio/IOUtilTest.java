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

package com.hazelcast.nio;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * @author Tomasz Nurkiewicz
 * @since 25.09.12, 12:03
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class IOUtilTest {

    private static final byte[] NON_EMPTY_BYTE_ARRAY = new byte[100];
    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
    private static final int SIZE = 3;

    @Test
    public void shouldReturnMinusOneWhenEmptyByteBufferProvidedAndReadingOneByte() throws Exception {
        //given
        final ByteBuffer empty = ByteBuffer.wrap(EMPTY_BYTE_ARRAY);
        final InputStream inputStream = IOUtil.newInputStream(empty);

        //when
        final int read = inputStream.read();

        //then
        assertEquals(-1, read);
    }

    @Test
    public void shouldReadWholeByteBuffer() throws Exception {
        //given
        final ByteBuffer empty = ByteBuffer.wrap(new byte[SIZE]);
        final InputStream inputStream = IOUtil.newInputStream(empty);

        //when
        final int read = inputStream.read(new byte[SIZE]);

        //then
        assertEquals(3, read);
    }

    @Test
    public void shouldAllowReadingByteBufferInChunks() throws Exception {
        //given
        final ByteBuffer buffer = ByteBuffer.wrap(new byte[SIZE]);
        final InputStream inputStream = IOUtil.newInputStream(buffer);

        //when
        final int firstRead = inputStream.read(new byte[1]);
        final int secondRead = inputStream.read(new byte[SIZE - 1]);

        //then
        assertEquals(1, firstRead);
        assertEquals(SIZE - 1, secondRead);
    }

    @Test
    public void shouldReturnMinusOneWhenNothingRemainingInByteBuffer() throws Exception {
        //given
        final int SIZE = 3;
        final ByteBuffer buffer = ByteBuffer.wrap(new byte[SIZE]);
        final InputStream inputStream = IOUtil.newInputStream(buffer);
        inputStream.read(new byte[SIZE]);

        //when
        final int read = inputStream.read();

        //then
        assertEquals(-1, read);
    }

    @Test
    public void shouldReturnMinusOneWhenEmptyByteBufferProvidedAndReadingSeveralBytes() throws Exception {
        //given
        final ByteBuffer empty = ByteBuffer.wrap(EMPTY_BYTE_ARRAY);
        final InputStream inputStream = IOUtil.newInputStream(empty);

        //when
        final int read = inputStream.read(NON_EMPTY_BYTE_ARRAY);

        //then
        assertEquals(-1, read);
    }

    @Test
    public void shouldThrowWhenTryingToReadFullyFromEmptyByteBuffer() throws Exception {
        //given
        final ByteBuffer empty = ByteBuffer.wrap(EMPTY_BYTE_ARRAY);
        final DataInputStream inputStream = new DataInputStream(IOUtil.newInputStream(empty));

        try {
            //when
            inputStream.readFully(NON_EMPTY_BYTE_ARRAY);
            fail("EOFException expected");
        }
        //then
        catch (EOFException e) {
        }
    }

    @Test
    public void shouldThrowWhenByteBufferExhaustedAndTryingToReadFully() throws Exception {
        //given
        final ByteBuffer buffer = ByteBuffer.wrap(new byte[SIZE]);
        final DataInputStream inputStream = new DataInputStream(IOUtil.newInputStream(buffer));
        inputStream.readFully(new byte[SIZE]);

        try {
            //when
            inputStream.readFully(NON_EMPTY_BYTE_ARRAY);
            fail("EOFException expected");
        }
        //then
        catch (EOFException e) {
        }
    }

}
