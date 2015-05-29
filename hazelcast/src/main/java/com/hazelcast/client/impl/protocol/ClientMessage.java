/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.protocol;

import com.hazelcast.client.impl.protocol.util.ClientProtocolBuffer;
import com.hazelcast.client.impl.protocol.util.MessageFlyweight;
import com.hazelcast.client.impl.protocol.util.SafeBuffer;
import com.hazelcast.client.impl.protocol.util.UnsafeBuffer;
import com.hazelcast.nio.Bits;
import com.hazelcast.nio.SocketReadable;
import com.hazelcast.nio.SocketWritable;
import com.hazelcast.util.QuickMath;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * <p>
 * Client Message is the carrier framed data as defined below.
 * </p>
 * <p>
 * Any request parameter, response or event data will be carried in
 * the payload.
 * </p>
 * <p/>
 * <pre>
 * 0                   1                   2                   3
 * 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |R|                      Frame Length                           |
 * +-------------+---------------+---------------------------------+
 * |  Version    |B|E|  Flags    |               Type              |
 * +-------------+---------------+---------------------------------+
 * |                       CorrelationId                           |
 * +---------------------------------------------------------------+
 * |                        PartitionId                            |
 * +-----------------------------+---------------------------------+
 * |        Data Offset          |                                 |
 * +-----------------------------+                                 |
 * |                      Message Payload Data                    ...
 * |                                                              ...
 *
 * </pre>
 */
public class ClientMessage
        extends MessageFlyweight
        implements SocketWritable, SocketReadable {


    /**
     * Current protocol version
     */
    public static final short VERSION = 0;

    /**
     * Begin Flag
     */
    public static final short BEGIN_FLAG = 0x80;

    /**
     * End Flag
     */
    public static final short END_FLAG = 0x40;

    /**
     * Begin and End Flags
     */
    public static final short BEGIN_AND_END_FLAGS = (short) (BEGIN_FLAG | END_FLAG);

    /**
     * Listener Event Flag
     */
    public static final short LISTENER_EVENT_FLAG = 0x01;

    /**
     * ClientMessage Fixed Header size in bytes
     */
    public static final int HEADER_SIZE;


    private static final String PROP_HAZELCAST_PROTOCOL_UNSAFE = "hazelcast.protocol.unsafe.enabled";
    private static final boolean USE_UNSAFE = Boolean.getBoolean(PROP_HAZELCAST_PROTOCOL_UNSAFE);
    private static final int INITIAL_BUFFER_SIZE = 1024;

    private static final int FRAME_LENGTH_FIELD_OFFSET = 0;
    private static final int VERSION_FIELD_OFFSET = FRAME_LENGTH_FIELD_OFFSET + Bits.INT_SIZE_IN_BYTES;
    private static final int FLAGS_FIELD_OFFSET = VERSION_FIELD_OFFSET + Bits.BYTE_SIZE_IN_BYTES;
    private static final int TYPE_FIELD_OFFSET = FLAGS_FIELD_OFFSET + Bits.BYTE_SIZE_IN_BYTES;
    private static final int CORRELATION_ID_FIELD_OFFSET = TYPE_FIELD_OFFSET + Bits.SHORT_SIZE_IN_BYTES;
    private static final int PARTITION_ID_FIELD_OFFSET = CORRELATION_ID_FIELD_OFFSET + Bits.INT_SIZE_IN_BYTES;
    private static final int DATA_OFFSET_FIELD_OFFSET = PARTITION_ID_FIELD_OFFSET + Bits.INT_SIZE_IN_BYTES;


    static {

        HEADER_SIZE = DATA_OFFSET_FIELD_OFFSET + Bits.SHORT_SIZE_IN_BYTES;
    }

    private transient int valueOffset;
    private transient boolean isRetryable;

    public ClientMessage() {
        super();
    }

    public static ClientMessage create() {
        final ClientMessage clientMessage = new ClientMessage();

        if (USE_UNSAFE) {
            clientMessage.wrap(new UnsafeBuffer(new byte[INITIAL_BUFFER_SIZE]), 0);
        } else {
            clientMessage.wrap(new SafeBuffer(new byte[INITIAL_BUFFER_SIZE]), 0);

        }
        return clientMessage;
    }

    public static ClientMessage createForEncode(int initialCapacity) {
        initialCapacity = QuickMath.nextPowerOfTwo(initialCapacity);
        if (USE_UNSAFE) {
            return createForEncode(new UnsafeBuffer(new byte[initialCapacity]), 0);
        } else {
            return createForEncode(new SafeBuffer(new byte[initialCapacity]), 0);
        }
    }

    public static ClientMessage createForEncode(ClientProtocolBuffer buffer, int offset) {
        ClientMessage clientMessage = new ClientMessage();
        clientMessage.wrapForEncode(buffer, offset);
        return clientMessage;
    }

    public static ClientMessage createForDecode(ClientProtocolBuffer buffer, int offset) {
        ClientMessage clientMessage = new ClientMessage();
        clientMessage.wrapForDecode(buffer, offset);
        return clientMessage;
    }

    protected void wrapForEncode(ClientProtocolBuffer buffer, int offset) {
        ensureHeaderSize(offset, buffer.capacity());
        super.wrap(buffer, offset);
        setDataOffset(HEADER_SIZE);
        setFrameLength(HEADER_SIZE);
        index(getDataOffset());
        setPartitionId(-1);
    }

    private void ensureHeaderSize(int offset, int length) {
        if (length - offset < HEADER_SIZE) {
            throw new IndexOutOfBoundsException("ClientMessage buffer must contain at least "
                    + HEADER_SIZE + " bytes! length: " + length + ", offset: " + offset);
        }
    }

    protected void wrapForDecode(ClientProtocolBuffer buffer, int offset) {
        ensureHeaderSize(offset, buffer.capacity());
        super.wrap(buffer, offset);
        index(getDataOffset());
    }

    /**
     * Returns the version field value.
     *
     * @return The version field value.
     */
    public short getVersion() {
        return uint8Get(VERSION_FIELD_OFFSET);
    }

    /**
     * Sets the version field value.
     *
     * @param version The value to set in the version field.
     * @return The ClientMessage with the new version field value.
     */
    public ClientMessage setVersion(final short version) {
        uint8Put(VERSION_FIELD_OFFSET, version);
        return this;
    }


    /**
     * @param flag Check this flag to see if it is set.
     * @return true if the given flag is set, false otherwise.
     */
    public boolean isFlagSet(short flag) {
        int i = getFlags() & flag;
        return i == flag;
    }

    /**
     * Returns the flags field value.
     *
     * @return The flags field value.
     */
    public short getFlags() {
        return uint8Get(FLAGS_FIELD_OFFSET);
    }

    /**
     * Sets the flags field value.
     *
     * @param flags The value to set in the flags field.
     * @return The ClientMessage with the new flags field value.
     */
    public ClientMessage addFlag(final short flags) {
        uint8Put(FLAGS_FIELD_OFFSET, (short) (getFlags() | flags));
        return this;
    }

    /**
     * Returns the message type field.
     *
     * @return The message type field value.
     */
    public int getMessageType() {
        return uint16Get(TYPE_FIELD_OFFSET);
    }

    /**
     * Sets the message type field.
     *
     * @param type The value to set in the message type field.
     * @return The ClientMessage with the new message type field value.
     */
    public ClientMessage setMessageType(final int type) {
        uint16Put(TYPE_FIELD_OFFSET, type);
        return this;
    }

    /**
     * Returns the frame length field.
     *
     * @return The frame length field.
     */
    public int getFrameLength() {
        return int32Get(FRAME_LENGTH_FIELD_OFFSET);
    }

    /**
     * Sets the frame length field.
     *
     * @param length The value to set in the frame length field.
     * @return The ClientMessage with the new frame length field value.
     */
    public ClientMessage setFrameLength(final int length) {
        int32Set(FRAME_LENGTH_FIELD_OFFSET, length);
        return this;
    }

    /**
     * Returns the correlation id field.
     *
     * @return The correlation id field.
     */
    public int getCorrelationId() {
        return int32Get(CORRELATION_ID_FIELD_OFFSET);
    }

    /**
     * Sets the correlation id field.
     *
     * @param correlationId The value to set in the correlation id field.
     * @return The ClientMessage with the new correlation id field value.
     */
    public ClientMessage setCorrelationId(final int correlationId) {
        int32Set(CORRELATION_ID_FIELD_OFFSET, correlationId);
        return this;
    }

    /**
     * Returns the partition id field.
     *
     * @return The partition id field.
     */
    public int getPartitionId() {
        return int32Get(PARTITION_ID_FIELD_OFFSET);
    }

    /**
     * Sets the partition id field.
     *
     * @param partitionId The value to set in the partitions id field.
     * @return The ClientMessage with the new partitions id field value.
     */
    public ClientMessage setPartitionId(final int partitionId) {
        int32Set(PARTITION_ID_FIELD_OFFSET, partitionId);
        return this;
    }

    /**
     * Returns the setDataOffset field.
     *
     * @return The setDataOffset type field value.
     */
    public int getDataOffset() {
        return uint16Get(DATA_OFFSET_FIELD_OFFSET);
    }

    /**
     * Sets the dataOffset field.
     *
     * @param dataOffset The value to set in the dataOffset field.
     * @return The ClientMessage with the new dataOffset field value.
     */
    public ClientMessage setDataOffset(final int dataOffset) {
        uint16Put(DATA_OFFSET_FIELD_OFFSET, dataOffset);
        return this;
    }

    public ClientMessage updateFrameLength() {
        setFrameLength(index());
        return this;
    }

    @Override
    public boolean writeTo(ByteBuffer destination) {
        byte[] byteArray = buffer.byteArray();
        int size = getFrameLength();

        // the number of bytes that can be written to the bb.
        int bytesWritable = destination.remaining();

        // the number of bytes that need to be written.
        int bytesNeeded = size - valueOffset;

        int bytesWrite;
        boolean done;
        if (bytesWritable >= bytesNeeded) {
            // All bytes for the value are available.
            bytesWrite = bytesNeeded;
            done = true;
        } else {
            // Not all bytes for the value are available. Write as much as is available.
            bytesWrite = bytesWritable;
            done = false;
        }

        destination.put(byteArray, valueOffset, bytesWrite);
        valueOffset += bytesWrite;

        return done;
    }

    public boolean readFrom(ByteBuffer source) {
        if (index() == 0) {
            initFrameSize(source);
        }
        while (index() >= Bits.INT_SIZE_IN_BYTES && source.hasRemaining() && !isComplete()) {
            accumulate(source, getFrameLength() - index());
        }
        return isComplete();
    }

    private int initFrameSize(ByteBuffer byteBuffer) {
        if (byteBuffer.remaining() < Bits.INT_SIZE_IN_BYTES) {
            return 0;
        }
        return accumulate(byteBuffer, Bits.INT_SIZE_IN_BYTES);
    }

    private int accumulate(ByteBuffer byteBuffer, int length) {
        final int remaining = byteBuffer.remaining();
        final int readLength = remaining < length ? remaining : length;
        if (readLength > 0) {
            final int requiredCapacity = index() + readLength;
            ensureCapacity(requiredCapacity);
            buffer.putBytes(index(), byteBuffer.array(), byteBuffer.position(), readLength);
            byteBuffer.position(byteBuffer.position() + readLength);
            index(index() + readLength);
            return readLength;
        }
        return 0;
    }

    /**
     * Checks the frame size and total data size to validate the message size.
     *
     * @return true if the message is constructed.
     */
    public boolean isComplete() {
        return (index() >= HEADER_SIZE) && (index() == getFrameLength());
    }

    @Override
    public boolean isUrgent() {
        return false;
    }

    public void setRetryable(boolean isRetryable) {
        this.isRetryable = isRetryable;
    }

    public boolean isRetryable() {
        return isRetryable;
    }

    @Override
    public String toString() {
        int len = index();
        final StringBuilder sb = new StringBuilder("ClientMessage{");
        sb.append("length=").append(len);
        if (len >= HEADER_SIZE) {
            sb.append(", correlationId=").append(getCorrelationId());
            sb.append(", messageType=").append(Integer.toHexString(getMessageType()));
            sb.append(", partitionId=").append(getPartitionId());
            sb.append(", isComplete=").append(isComplete());
            sb.append(", isEvent=").append(isFlagSet(LISTENER_EVENT_FLAG));
        }
        sb.append('}');
        return sb.toString();
    }

    private void ensureCapacity(int requiredCapacity) {
        int capacity = buffer.capacity() > 0 ? buffer.capacity() : 1;
        if (requiredCapacity > capacity) {
            int newCapacity = QuickMath.nextPowerOfTwo(requiredCapacity);
            byte[] newBuffer = Arrays.copyOf(buffer.byteArray(), newCapacity);
            buffer.wrap(newBuffer);
        }
    }
}
