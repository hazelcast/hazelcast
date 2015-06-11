/*
 * Copyright 2014 - 2015 Real Logic Ltd.
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
package com.hazelcast.client.impl.protocol.util;

import com.hazelcast.util.QuickMath;

import java.util.Arrays;

/**
 * Builder for appending buffers that grows capacity as necessary.
 */
public class BufferBuilder {
    public static final int INITIAL_CAPACITY = 4096;

    private static final String PROP_HAZELCAST_PROTOCOL_UNSAFE = "hazelcast.protocol.unsafe.enabled";
    private static final boolean USE_UNSAFE = Boolean.getBoolean(PROP_HAZELCAST_PROTOCOL_UNSAFE);

    private final ClientProtocolBuffer protocolBuffer;

    private int position;
    private int capacity;

    /**
     * Construct a buffer builder with a default growth increment of {@link #INITIAL_CAPACITY}
     */
    public BufferBuilder() {
        this(INITIAL_CAPACITY);
    }

    /**
     * Construct a buffer builder with an initial capacity that will be rounded up to the nearest power of 2.
     *
     * @param initialCapacity at which the capacity will start.
     */
    private BufferBuilder(int initialCapacity) {
        capacity = QuickMath.nextPowerOfTwo(initialCapacity);
        if (USE_UNSAFE) {
            protocolBuffer = new UnsafeBuffer(new byte[capacity]);
        } else {
            protocolBuffer = new SafeBuffer(new byte[capacity]);

        }
    }

    /**
     * The current capacity of the buffer.
     *
     * @return the current capacity of the buffer.
     */
    public int capacity() {
        return capacity;
    }

    /**
     * The current position of the buffer that has been used by accumulate operations.
     *
     * @return the current position of the buffer that has been used by accumulate operations.
     */
    public int position() {
        return position;
    }

    /**
     * The {@link ClientProtocolBuffer} that encapsulates the internal buffer.
     *
     * @return the {@link ClientProtocolBuffer} that encapsulates the internal buffer.
     */
    public ClientProtocolBuffer buffer() {
        return protocolBuffer;
    }

    /**
     * Append a source buffer to the end of the internal buffer, resizing the internal buffer as required.
     *
     * @param srcBuffer from which to copy.
     * @param srcOffset in the source buffer from which to copy.
     * @param length    in bytes to copy from the source buffer.
     * @return the builder for fluent API usage.
     */
    public BufferBuilder append(ClientProtocolBuffer srcBuffer, int srcOffset, int length) {
        ensureCapacity(length);

        srcBuffer.getBytes(srcOffset, protocolBuffer.byteArray(), position, length);
        position += length;

        return this;
    }

    private void ensureCapacity(int additionalCapacity) {
        int requiredCapacity = position + additionalCapacity;

        if (requiredCapacity < 0) {
            String s = String.format("Insufficient capacity: position=%d additional=%d", position, additionalCapacity);
            throw new IllegalStateException(s);
        }

        if (requiredCapacity > capacity) {
            int newCapacity = QuickMath.nextPowerOfTwo(requiredCapacity);
            byte[] newBuffer = Arrays.copyOf(protocolBuffer.byteArray(), newCapacity);

            capacity = newCapacity;
            protocolBuffer.wrap(newBuffer);
        }
    }
}
