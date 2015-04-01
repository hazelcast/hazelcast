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
        implements SocketWritable {

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
     * return version field value
     *
     * @return ver field value
     */
    public short getVersion() {
        return uint8Get(offset() + VERSION_FIELD_OFFSET);
    }

    /**
     * set version field value
     *
     * @param ver field value
     * @return ClientMessage
     */
    public ClientMessage setVersion(final short ver) {
        uint8Put(offset() + VERSION_FIELD_OFFSET, ver);
        return this;
    }

    /**
     * return flags field value
     *
     * @return flags field value
     */
    public short getFlags() {
        return uint8Get(offset() + FLAGS_FIELD_OFFSET);
    }

    /**
     * set the flags field value
     *
     * @param flags field value
     * @return ClientMessage
     */
    public ClientMessage setFlags(final short flags) {
        uint8Put(offset() + FLAGS_FIELD_OFFSET, flags);
        return this;
    }

    /**
     * return message type field
     *
     * @return type field value
     */
    public int getMessageType() {
        return uint16Get(offset() + TYPE_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * set message type field
     *
     * @param type field value
     * @return ClientMessage
     */
    protected ClientMessage setMessageType(final int type) {
        uint16Put(offset() + TYPE_FIELD_OFFSET, (short) type, LITTLE_ENDIAN);
        return this;
    }

    /**
     * return frame length field
     *
     * @return frame length field
     */
    public int getFrameLength() {
        return (int) uint32Get(offset() + FRAME_LENGTH_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * set frame length field
     *
     * @param length field value
     * @return ClientMessage
     */
    public ClientMessage setFrameLength(final int length) {
        uint32Put(offset() + FRAME_LENGTH_FIELD_OFFSET, length, LITTLE_ENDIAN);
        return this;
    }

    /**
     * return correlation id field
     *
     * @return correlation id field
     */
    public int getCorrelationId() {
        return (int) uint32Get(offset() + CORRELATION_ID_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * set correlation id field
     *
     * @param correlationId field value
     * @return ClientMessage
     */
    public ClientMessage setCorrelationId(final int correlationId) {
        uint32Put(offset() + CORRELATION_ID_FIELD_OFFSET, correlationId, ByteOrder.LITTLE_ENDIAN);
        return this;
    }

    /**
     * return partition id field
     *
     * @return partition id field
     */
    public int getPartitionId() {
        return (int) uint32Get(offset() + PARTITION_ID_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * set partition id field
     *
     * @param partitionId field value
     * @return ClientMessage
     */
    public ClientMessage setPartitionId(final int partitionId) {
        uint32Put(offset() + PARTITION_ID_FIELD_OFFSET, partitionId, ByteOrder.LITTLE_ENDIAN);
        return this;
    }

    /**
     * return setDataOffset field
     *
     * @return type field value
     */
    public int getDataOffset() {
        return uint16Get(offset() + DATA_OFFSET_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * set dataOffset field
     *
     * @param dataOffset field value
     * @return ClientMessage
     */
    public ClientMessage setDataOffset(final int dataOffset) {
        uint16Put(offset() + DATA_OFFSET_FIELD_OFFSET, (short) dataOffset, LITTLE_ENDIAN);
        return this;
    }

    /**
     * copy into payload data region located at data Offset
     *
     * @param payload the payload data
     * @return ClientMessage
     */
    public ClientMessage putPayloadData(byte[] payload) {
        final int index = offset() + getDataOffset();
        buffer.putBytes(index, payload);
        setFrameLength(getFrameLength() + payload.length);
        return this;
    }

    /**
     * reads from payload data region into the provided array
     *
     * @param payload destination array that payload will be copied into
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

    @Override
    public boolean isUrgent() {
        return false;
    }
}
