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

package com.hazelcast.client.impl.protocol;

import com.hazelcast.client.impl.protocol.exception.MaxMessageSizeExceeded;
import com.hazelcast.client.impl.protocol.util.BufferBuilder;
import com.hazelcast.client.impl.protocol.util.ClientProtocolBuffer;
import com.hazelcast.client.impl.protocol.util.MessageFlyweight;
import com.hazelcast.client.impl.protocol.util.SafeBuffer;
import com.hazelcast.client.impl.protocol.util.UnsafeBuffer;
import com.hazelcast.internal.networking.OutboundFrame;
import com.hazelcast.nio.Bits;
import com.hazelcast.nio.Connection;

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
 * |                                                               |
 * +                       CorrelationId                           +
 * |                                                               |
 * +---------------------------------------------------------------+
 * |                        PartitionId                            |
 * +-----------------------------+---------------------------------+
 * |        Data Offset          |                                 |
 * +-----------------------------+                                 |
 * |                      Message Payload Data                    ...
 * |                                                              ...
 * </pre>
 */
public class ClientMessage
        extends MessageFlyweight
        implements OutboundFrame {

    /**
     * Current protocol version
     */
    public static final short VERSION = 1;

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

    private static final int FRAME_LENGTH_FIELD_OFFSET = 0;
    private static final int VERSION_FIELD_OFFSET = FRAME_LENGTH_FIELD_OFFSET + Bits.INT_SIZE_IN_BYTES;
    private static final int FLAGS_FIELD_OFFSET = VERSION_FIELD_OFFSET + Bits.BYTE_SIZE_IN_BYTES;
    private static final int TYPE_FIELD_OFFSET = FLAGS_FIELD_OFFSET + Bits.BYTE_SIZE_IN_BYTES;
    private static final int CORRELATION_ID_FIELD_OFFSET = TYPE_FIELD_OFFSET + Bits.SHORT_SIZE_IN_BYTES;
    private static final int PARTITION_ID_FIELD_OFFSET = CORRELATION_ID_FIELD_OFFSET + Bits.LONG_SIZE_IN_BYTES;
    private static final int DATA_OFFSET_FIELD_OFFSET = PARTITION_ID_FIELD_OFFSET + Bits.INT_SIZE_IN_BYTES;

    static {

        HEADER_SIZE = DATA_OFFSET_FIELD_OFFSET + Bits.SHORT_SIZE_IN_BYTES;
    }

    private transient int writeOffset;
    private transient boolean isRetryable;
    private transient boolean acquiresResource;
    private transient String operationName;
    private Connection connection;

    protected ClientMessage() {
    }

    public Connection getConnection() {
        return connection;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    protected void wrapForEncode(ClientProtocolBuffer buffer, int offset) {
        ensureHeaderSize(offset, buffer.capacity());
        super.wrap(buffer.byteArray(), offset, USE_UNSAFE);
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
        super.wrap(buffer.byteArray(), offset, USE_UNSAFE);
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
     * Returns the correlation ID field.
     *
     * @return The correlation ID field.
     */
    public long getCorrelationId() {
        return int64Get(CORRELATION_ID_FIELD_OFFSET);
    }

    /**
     * Sets the correlation ID field.
     *
     * @param correlationId The value to set in the correlation ID field.
     * @return The ClientMessage with the new correlation ID field value.
     */
    public ClientMessage setCorrelationId(final long correlationId) {
        int64Set(CORRELATION_ID_FIELD_OFFSET, correlationId);
        return this;
    }

    /**
     * Returns the partition ID field.
     *
     * @return The partition ID field.
     */
    public int getPartitionId() {
        return int32Get(PARTITION_ID_FIELD_OFFSET);
    }

    /**
     * Sets the partition ID field.
     *
     * @param partitionId The value to set in the partitions ID field.
     * @return The ClientMessage with the new partitions ID field value.
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

    public boolean writeTo(ByteBuffer dst) {
        byte[] byteArray = buffer.byteArray();
        int size = getFrameLength();

        // the number of bytes that can be written to the bb
        int bytesWritable = dst.remaining();

        // the number of bytes that need to be written
        int bytesNeeded = size - writeOffset;

        int bytesWrite;
        boolean done;
        if (bytesWritable >= bytesNeeded) {
            // all bytes for the value are available
            bytesWrite = bytesNeeded;
            done = true;
        } else {
            // not all bytes for the value are available. Write as much as is available
            bytesWrite = bytesWritable;
            done = false;
        }

        dst.put(byteArray, writeOffset, bytesWrite);
        writeOffset += bytesWrite;

        if (done) {
            // clear the write offset so that same client message can be resend if needed
            writeOffset = 0;
        }
        return done;
    }

    public boolean readFrom(ByteBuffer src) {
        int frameLength = 0;
        if (this.buffer == null) {
            // init internal buffer
            final int remaining = src.remaining();
            if (remaining < Bits.INT_SIZE_IN_BYTES) {
                // we don't have even the frame length ready
                return false;
            }
            frameLength = Bits.readIntL(src);
            // we need to restore the position; as if we didn't read the frame-length
            src.position(src.position() - Bits.INT_SIZE_IN_BYTES);
            if (frameLength < HEADER_SIZE) {
                throw new IllegalArgumentException("Client message frame length cannot be smaller than header size.");
            }
            wrap(new byte[frameLength], 0, USE_UNSAFE);
        }
        frameLength = frameLength > 0 ? frameLength : getFrameLength();
        accumulate(src, frameLength - index());
        return isComplete();
    }

    private int accumulate(ByteBuffer src, int length) {
        int remaining = src.remaining();
        int readLength = remaining < length ? remaining : length;
        if (readLength > 0) {
            buffer.putBytes(index(), src, readLength);
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

    public boolean isRetryable() {
        return isRetryable;
    }

    public boolean acquiresResource() {
        return acquiresResource;
    }

    public void setAcquiresResource(boolean acquiresResource) {
        this.acquiresResource = acquiresResource;
    }

    public void setRetryable(boolean isRetryable) {
        this.isRetryable = isRetryable;
    }

    public void setOperationName(String operationName) {
        this.operationName = operationName;
    }

    public String getOperationName() {
        return operationName;
    }

    @Override
    public String toString() {
        int len = index();
        final StringBuilder sb = new StringBuilder("ClientMessage{");
        sb.append("connection=").append(connection);
        sb.append(", length=").append(len);
        if (len >= HEADER_SIZE) {
            sb.append(", correlationId=").append(getCorrelationId());
            sb.append(", operation=").append(operationName);
            sb.append(", messageType=").append(Integer.toHexString(getMessageType()));
            sb.append(", partitionId=").append(getPartitionId());
            sb.append(", isComplete=").append(isComplete());
            sb.append(", isRetryable=").append(isRetryable());
            sb.append(", isEvent=").append(isFlagSet(LISTENER_EVENT_FLAG));
            sb.append(", writeOffset=").append(writeOffset);
        }
        sb.append('}');
        return sb.toString();
    }

    public static ClientMessage create() {
        return new ClientMessage();
    }

    public static ClientMessage createForEncode(int initialCapacity) {
        if (initialCapacity < 0) {
            throw new MaxMessageSizeExceeded();
        }
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

    public ClientMessage copy() {
        byte[] oldBinary = buffer().byteArray();
        byte[] bytes = Arrays.copyOf(oldBinary, oldBinary.length);
        ClientMessage newMessage = ClientMessage.createForDecode(BufferBuilder.createBuffer(bytes), 0);
        newMessage.isRetryable = isRetryable;
        newMessage.acquiresResource = acquiresResource;
        newMessage.operationName = operationName;
        return newMessage;
    }

    @Override
    public int hashCode() {
        return ByteBuffer.wrap(buffer().byteArray(), 0, getFrameLength()).hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ClientMessage that = (ClientMessage) o;

        byte[] thisBytes = this.buffer().byteArray();
        byte[] thatBytes = that.buffer().byteArray();
        if (this.getFrameLength() != that.getFrameLength()) {
            return false;
        }
        for (int i = 0; i < this.getFrameLength(); i++) {
            if (thisBytes[i] != thatBytes[i]) {
                return false;
            }
        }
        return true;
    }
}
