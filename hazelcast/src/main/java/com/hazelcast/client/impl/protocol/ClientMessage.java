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

import com.hazelcast.client.impl.protocol.util.BitUtil;
import com.hazelcast.client.impl.protocol.util.ParameterFlyweight;
import com.hazelcast.nio.SocketReadable;
import com.hazelcast.nio.SocketWritable;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static java.nio.ByteOrder.LITTLE_ENDIAN;

/**
 *
 * <p>
 *     Client Message is the carrier framed data as defined below.
 * </p>
 * <p>
 *     Any request parameter, response or event data will be carried in
 *     the payload.
 * </p>
 *
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
 * |R|                      PartitionId                            |
 * +-----------------------------+---------------------------------+
 * |        Data Offset          |                                 |
 * +-----------------------------+                                 |
 * |                      Message Payload Data                    ...
 * |                                                              ...
 *
 * </pre>
 */
public class ClientMessage
        extends ParameterFlyweight
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
     * ClientMessage Fixed Header size in bytes
     */
    public static final int HEADER_SIZE;

    private static final int FRAME_LENGTH_FIELD_OFFSET = 0;
    private static final int VERSION_FIELD_OFFSET = FRAME_LENGTH_FIELD_OFFSET + BitUtil.SIZE_OF_INT;
    private static final int FLAGS_FIELD_OFFSET = VERSION_FIELD_OFFSET + BitUtil.SIZE_OF_BYTE;
    private static final int TYPE_FIELD_OFFSET = FLAGS_FIELD_OFFSET + BitUtil.SIZE_OF_BYTE;
    private static final int CORRELATION_ID_FIELD_OFFSET = TYPE_FIELD_OFFSET + BitUtil.SIZE_OF_SHORT;
    private static final int PARTITION_ID_FIELD_OFFSET = CORRELATION_ID_FIELD_OFFSET + BitUtil.SIZE_OF_INT;
    private static final int DATA_OFFSET_FIELD_OFFSET = PARTITION_ID_FIELD_OFFSET + BitUtil.SIZE_OF_INT;

    static {

        HEADER_SIZE = DATA_OFFSET_FIELD_OFFSET + BitUtil.SIZE_OF_SHORT;
    }

    private transient int valueOffset;

    protected ClientMessage() {
        super();
    }

    protected ClientMessage(boolean encode, final ByteBuffer buffer, final int offset) {
        super(buffer, offset);
        if (encode) {
            wrapForEncode(buffer, offset);
        } else {
            wrapForDecode(buffer, offset);
        }
    }

    public static ClientMessage create() {
        final ClientMessage clientMessage = new ClientMessage();
        clientMessage.wrap(ByteBuffer.allocate(INITIAL_BUFFER_CAPACITY), 0);
        return clientMessage;
    }

    public static ClientMessage createForEncode(int initialCapacity) {
        return new ClientMessage(true, ByteBuffer.allocate(initialCapacity), 0);
    }

    public static ClientMessage createForDecode(int initialCapacity) {
        return new ClientMessage(false, ByteBuffer.allocate(initialCapacity), 0);
    }

    public static ClientMessage createForEncode(final ByteBuffer buffer, final int offset) {
        return new ClientMessage(true, buffer, offset);
    }

    public static ClientMessage createForDecode(final ByteBuffer buffer, final int offset) {
        return new ClientMessage(false, buffer, offset);
    }

    public void wrapForEncode(final ByteBuffer buffer, final int offset) {
        super.wrap(buffer, offset);
        setDataOffset(HEADER_SIZE);
        setFrameLength(HEADER_SIZE);
        index(getDataOffset() + offset);
        setPartitionId(-1);
    }

    public void wrapForDecode(final ByteBuffer buffer, final int offset) {
        super.wrap(buffer, offset);
        index(getDataOffset() + offset);
    }

    /**
     * Returns the version field value.
     *
     * @return The version field value.
     */
    public short getVersion() {
        return uint8Get(offset() + VERSION_FIELD_OFFSET);
    }

    /**
     * Sets the version field value.
     *
     * @param version The field value to set.
     * @return ClientMessage
     */
    public ClientMessage setVersion(final short ver) {
        uint8Put(offset() + VERSION_FIELD_OFFSET, ver);
        return this;
    }

    /**
     * Returns the flags field value.
     *
     * @return The flags field value.
     */
    public short getFlags() {
        return uint8Get(offset() + FLAGS_FIELD_OFFSET);
    }

    /**
     * Sets the flags field value.
     *
     * @param flags The field value to set.
     * @return ClientMessage
     */
    public ClientMessage setFlags(final short flags) {
        uint8Put(offset() + FLAGS_FIELD_OFFSET, flags);
        return this;
    }

    /**
     * Returns the message type field.
     *
     * @return The message type field value.
     */
    public int getMessageType() {
        return uint16Get(offset() + TYPE_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * Sets the message type field.
     *
     * @param message The message type field value to set.
     * @return ClientMessage
     */
    public ClientMessage setMessageType(final int type) {
        uint16Put(offset() + TYPE_FIELD_OFFSET, (short) type, LITTLE_ENDIAN);
        return this;
    }

    /**
     * Returns the frame length field.
     *
     * @return The frame length field.
     */
    public int getFrameLength() {
        return (int) uint32Get(offset() + FRAME_LENGTH_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * Sets the frame length field.
     *
     * @param frame The frame length field value to set.
     * @return ClientMessage
     */
    public ClientMessage setFrameLength(final int length) {
        uint32Put(offset() + FRAME_LENGTH_FIELD_OFFSET, length, LITTLE_ENDIAN);
        return this;
    }

    /**
     * Returns the correlation id field.
     *
     * @return The correlation id field.
     */
    public int getCorrelationId() {
        return (int) uint32Get(offset() + CORRELATION_ID_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * Sets the correlation id field.
     *
     * @param correlationId The correlation id field value to set.
     * @return ClientMessage
     */
    public ClientMessage setCorrelationId(final int correlationId) {
        uint32Put(offset() + CORRELATION_ID_FIELD_OFFSET, correlationId, ByteOrder.LITTLE_ENDIAN);
        return this;
    }

    /**
     * Returns the partition id field.
     *
     * @return The partition id field.
     */
    public int getPartitionId() {
        return (int) uint32Get(offset() + PARTITION_ID_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * Sets the partition id field.
     *
     * @param partitionId The partitions id field value to set.
     * @return ClientMessage
     */
    public ClientMessage setPartitionId(final int partitionId) {
        uint32Put(offset() + PARTITION_ID_FIELD_OFFSET, partitionId, ByteOrder.LITTLE_ENDIAN);
        return this;
    }

    /**
     * Returns the setDataOffset field.
     *
     * @return The setDataOffset type field value.
     */
    public int getDataOffset() {
        return uint16Get(offset() + DATA_OFFSET_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * Sets the dataOffset field.
     *
     * @param dataOffset The dataOffset field value to set.
     * @return ClientMessage
     */
    public ClientMessage setDataOffset(final int dataOffset) {
        uint16Put(offset() + DATA_OFFSET_FIELD_OFFSET, (short) dataOffset, LITTLE_ENDIAN);
        return this;
    }

    /**
     * Copy into the payload data region located at data Offset.
     *
     * @param payload The data being copied into the payload data region.
     * @return ClientMessage
     */
    public ClientMessage putPayloadData(byte[] payload) {
        final int index = offset() + getDataOffset();
        buffer.putBytes(index, payload);
        setFrameLength(getFrameLength() + payload.length);
        return this;
    }

    /**
     * Reads from the payload data region into the provided array.
     *
     * @param The payload destination array that the payload will be copied into.
     * @return ClientMessage
     */
    public ClientMessage getPayloadData(byte[] payload) {
        final int index = offset() + getDataOffset();
        if (index >= (offset() + getFrameLength())) {
            throw new IndexOutOfBoundsException("index cannot exceed frame length");
        }
        final int length = (offset() + getFrameLength()) - index;
        buffer.getBytes(index, payload, 0, length);
        return this;
    }

    public ClientMessage updateFrameLenght() {
        setFrameLength(index());
        return this;
    }

    @Override
    public boolean writeTo(ByteBuffer destination) {
        byte[] byteArray = buffer().byteArray();
        int size = byteArray.length;

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
            // Not all bytes for the value are available. So lets write as much as is available.
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
        while (index() >= BitUtil.SIZE_OF_INT && source.hasRemaining() && !isComplete()) {
            accumulate(source, getFrameLength() - index());
        }
        return isComplete();
    }

    private int initFrameSize(ByteBuffer byteBuffer) {
        if (byteBuffer.remaining() < BitUtil.SIZE_OF_INT) {
            return 0;
        }
        final int accumulatedBytesSize = accumulate(byteBuffer, BitUtil.SIZE_OF_INT);
        return accumulatedBytesSize;
    }

    private int accumulate(ByteBuffer byteBuffer, int length) {
        final int remaining = byteBuffer.remaining();
        final int readLength = remaining < length ? remaining : length;
        if (readLength > 0) {
            final int requiredCapacity = index() + readLength;
            ensureCapacity(requiredCapacity);
            buffer.putBytes(index(), byteBuffer, readLength);
            index(index() + readLength);
            return readLength;
        }
        return 0;
    }

    /**
     * Checks the frame size and total data size to validate the message size.
     * @return true if the message is constructed.
     */
    public boolean isComplete() {
        return (index() > HEADER_SIZE) && (index() == getFrameLength());
    }

    @Override
    public boolean isUrgent() {
        return false;
    }
}
