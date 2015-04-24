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
import com.hazelcast.client.impl.protocol.util.MutableDirectBuffer;
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
     * Listener Event Flag
     */
    public static final short LISTENER_EVENT_FLAG = 0x01;

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

    protected ClientMessage(boolean encode, byte[] buffer, int offset, int length) {
        super(buffer, offset, length);
        if (encode) {
            wrapForEncode(buffer, offset, length);
        } else {
            wrapForDecode(buffer, offset, length);
        }
    }

    public ClientMessage(boolean encode, MutableDirectBuffer buffer, int offset) {
        super(buffer, offset);
        if (encode) {
            setDataOffset(HEADER_SIZE);
            setFrameLength(HEADER_SIZE);
            setPartitionId(-1);
        }
        index(getDataOffset() + offset);
    }

    public static ClientMessage create() {
        final ClientMessage clientMessage = new ClientMessage();
        clientMessage.wrap(new byte[INITIAL_BUFFER_CAPACITY]);
        return clientMessage;
    }

    public static ClientMessage createForEncode(int initialCapacity) {
        return new ClientMessage(true, new byte[initialCapacity], 0, initialCapacity);
    }

    public static ClientMessage createForDecode(int initialCapacity) {
        return new ClientMessage(false, new byte[initialCapacity], 0, initialCapacity);
    }

    public static ClientMessage createForEncode(byte[] buffer, final int offset, int length) {
        return new ClientMessage(true, buffer, offset, length);
    }

    public static ClientMessage createForDecode(byte[] buffer, final int offset, int length) {
        return new ClientMessage(false, buffer, offset, length);
    }

    public static ClientMessage createForDecode(final MutableDirectBuffer buffer, final int offset) {
        return new ClientMessage(false, buffer, offset);
    }

    public void wrapForEncode(byte[] buffer, int offset, int length) {
        super.wrap(buffer, offset, length);
        setDataOffset(HEADER_SIZE);
        setFrameLength(HEADER_SIZE);
        index(getDataOffset() + offset);
        setPartitionId(-1);
    }

    public void wrapForDecode(byte[] buffer, int offset, int length) {
        super.wrap(buffer, offset, length);
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
     * @param version The value to set in the version field.
     * @return The ClientMessage with the new version field value.
     */
    public ClientMessage setVersion(final short version) {
        uint8Put(offset() + VERSION_FIELD_OFFSET, version);
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
        return uint8Get(offset() + FLAGS_FIELD_OFFSET);
    }

    /**
     * Sets the flags field value.
     *
     * @param flags The value to set in the flags field.
     * @return The ClientMessage with the new flags field value.
     */
    public ClientMessage addFlag(final short flags) {
        uint8Put(offset() + FLAGS_FIELD_OFFSET, (short) (getFlags() | flags));
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
     * @param type The value to set in the message type field.
     * @return The ClientMessage with the new message type field value.
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
     * @param length The value to set in the frame length field.
     * @return The ClientMessage with the new frame length field value.
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
     * @param correlationId The value to set in the correlation id field.
     * @return The ClientMessage with the new correlation id field value.
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
     * @param partitionId The value to set in the partitions id field.
     * @return The ClientMessage with the new partitions id field value.
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
     * @param dataOffset The value to set in the dataOffset field.
     * @return The ClientMessage with the new dataOffset field value.
     */
    public ClientMessage setDataOffset(final int dataOffset) {
        uint16Put(offset() + DATA_OFFSET_FIELD_OFFSET, (short) dataOffset, LITTLE_ENDIAN);
        return this;
    }

    /**
     * Copy data into the payload data region located at data Offset.
     *
     * @param payload The data being copied into the ClientMessage payload data region.
     * @return The ClientMessage with the new data in its payload data region.
     */
    public ClientMessage putPayloadData(byte[] payload) {
        final int index = offset() + getDataOffset();
        buffer.putBytes(index, payload);
        setFrameLength(getFrameLength() + payload.length);
        return this;
    }

    /**
     * Reads from the ClientMessage payload data region and writes into the payload array.
     *
     * @param payload The payload array into which the payload data region in the ClientMessage will be copied.
     * @return The ClientMessage from which its payload data region is copied into the payload array.
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

    public ClientMessage updateFrameLength() {
        setFrameLength(index());
        return this;
    }

    @Override
    public boolean writeTo(ByteBuffer destination) {
        byte[] byteArray = buffer().byteArray();
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
        while (index() >= BitUtil.SIZE_OF_INT && source.hasRemaining() && !isComplete()) {
            accumulate(source, getFrameLength() - index());
        }
        return isComplete();
    }

    private int initFrameSize(ByteBuffer byteBuffer) {
        if (byteBuffer.remaining() < BitUtil.SIZE_OF_INT) {
            return 0;
        }
        return accumulate(byteBuffer, BitUtil.SIZE_OF_INT);
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
        return (index() >= HEADER_SIZE) && (index() == getFrameLength());
    }

    @Override
    public boolean isUrgent() {
        return false;
    }


    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClientMessage{");
        sb.append("correlationId=").append(getCorrelationId());
        sb.append(", messageType=").append(getMessageType());
        sb.append(", partitionId=").append(getPartitionId());
        sb.append(", isComplete=").append(isComplete());
        sb.append(", isEvent=").append(isFlagSet(LISTENER_EVENT_FLAG));
        sb.append('}');
        return sb.toString();
    }
}
