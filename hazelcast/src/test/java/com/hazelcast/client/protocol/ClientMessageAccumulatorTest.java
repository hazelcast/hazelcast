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

package com.hazelcast.client.protocol;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.util.ClientProtocolBuffer;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * ClientMessageAccumulator Tests
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientMessageAccumulatorTest {


    private static final byte[] BYTE_DATA = new byte[]{0, 0, 24, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
    private static final int OFFSET = 2;

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    @Test
    public void shouldAccumulateClientMessageCorrectly() {
        ClientMessage accumulator = ClientMessage.create();
        final ByteBuffer inBuffer = ByteBuffer.wrap(BYTE_DATA);
        inBuffer.position(OFFSET);
        accumulator.readFrom(inBuffer);

        final ByteBuffer byteBuffer = accumulatedByteBuffer(accumulator.buffer(), accumulator.index());
        assertEquals(0, byteBuffer.position());
        assertEquals(accumulator.getFrameLength(), byteBuffer.limit());

        for (int i = OFFSET; i < byteBuffer.limit(); i++) {
            assertEquals(BYTE_DATA[i], byteBuffer.get());
        }
        assertTrue(accumulator.isComplete());
    }

    @Test
    public void shouldNotAccumulateInCompleteFrameSize() {
        ClientMessage accumulator = ClientMessage.create();
        final byte[] array = new byte[]{1, 2, 3};
        final ByteBuffer inBuffer = ByteBuffer.wrap(array);
        assertFalse(accumulator.readFrom(inBuffer));
        assertFalse(accumulator.isComplete());
    }

    /**
     * setup the wrapped bytebuffer to point to this clientMessages data
     *
     * @return
     */
    static ByteBuffer accumulatedByteBuffer(final ClientProtocolBuffer buffer, int index) {
        if (buffer != null) {
            ByteBuffer byteBuffer = ByteBuffer.wrap(buffer.byteArray());
            byteBuffer.limit(index);
            byteBuffer.position(index);
            byteBuffer.flip();
            return byteBuffer;
        }
        return null;
    }

}
