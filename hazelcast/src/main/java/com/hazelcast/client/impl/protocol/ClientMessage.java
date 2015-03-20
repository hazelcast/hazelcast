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

import uk.co.real_logic.aeron.common.Flyweight;

import java.nio.ByteOrder;

import static java.nio.ByteOrder.LITTLE_ENDIAN;


/**
 * <pre>
 * 0                   1                   2                   3
 * 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |R|                      Frame Length                           |
 * +---------------------------------------------------------------+
 * |R|                     CorrelationId                           |
 * +-------------+---------------+---------------------------------+
 * |  Version    |     Flags     |               Type              |
 * +-------------+---------------+---------------------------------+
 * |        Data Offset          |                                 |
 * +-----------------------------+
 * |                       Message Payload Data                  ...
 *
 * </pre>
 */
public class ClientMessage extends Flyweight {

    public static final int FRAME_LENGTH_FIELD_OFFSET = 0;
    public static final int VERSION_FIELD_OFFSET = 4;
    public static final int FLAGS_FIELD_OFFSET = 5;
    public static final int TYPE_FIELD_OFFSET = 6;
    public static final int CORRELATION_ID_FIELD_OFFSET = 6;
    public static final int DATA_OFFSET_FIELD_OFFSET = 12;

    /** Begin Flag */
    public static final short BEGIN_FLAG = 0x80;

    /** End Flag */
    public static final short END_FLAG = 0x40;

    /** Begin and End Flags */
    public static final short BEGIN_AND_END_FLAGS = BEGIN_FLAG | END_FLAG;

    /**
     * return version field value
     *
     * @return ver field value
     */
    public short version()
    {
        return uint8Get(offset() + VERSION_FIELD_OFFSET);
    }

    /**
     * set version field value
     *
     * @param ver field value
     * @return ClientMessage
     */
    public ClientMessage version(final short ver)
    {
        uint8Put(offset() + VERSION_FIELD_OFFSET, ver);
        return this;
    }

    /**
     * return flags field value
     *
     * @return flags field value
     */
    public short flags()
    {
        return uint8Get(offset() + FLAGS_FIELD_OFFSET);
    }

    /**
     * set the flags field value
     *
     * @param flags field value
     * @return ClientMessage
     */
    public ClientMessage flags(final short flags)
    {
        uint8Put(offset() + FLAGS_FIELD_OFFSET, flags);
        return this;
    }

    /**
     * return header type field
     *
     * @return type field value
     */
    public int headerType()
    {
        return uint16Get(offset() + TYPE_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * set header type field
     *
     * @param type field value
     * @return ClientMessage
     */
    public ClientMessage headerType(final int type)
    {
        uint16Put(offset() + TYPE_FIELD_OFFSET, (short)type, LITTLE_ENDIAN);
        return this;
    }

    /**
     * return frame length field
     *
     * @return frame length field
     */
    public int frameLength()
    {
        return (int)uint32Get(offset() + FRAME_LENGTH_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * set frame length field
     *
     * @param length field value
     * @return ClientMessage
     */
    public ClientMessage frameLength(final int length)
    {
        uint32Put(offset() + FRAME_LENGTH_FIELD_OFFSET, length, LITTLE_ENDIAN);
        return this;
    }

    /**
     * return correlation id field
     *
     * @return correlation id field
     */
    public long correlationId()
    {
        return buffer().getLong(offset() + CORRELATION_ID_FIELD_OFFSET, ByteOrder.LITTLE_ENDIAN);
    }

    /**
     * set correlation id field
     *
     * @param correlationId field value
     * @return ClientMessage
     */
    public ClientMessage correlationId(final long correlationId)
    {
        buffer().putLong(offset() + CORRELATION_ID_FIELD_OFFSET, correlationId, ByteOrder.LITTLE_ENDIAN);
        return this;
    }

    /**
     * return dataOffset field
     *
     * @return type field value
     */
    public int dataOffset()
    {
        return uint16Get(offset() + DATA_OFFSET_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * set dataOffset field
     *
     * @param dataOffset field value
     * @return ClientMessage
     */
    public ClientMessage dataOffset(final int dataOffset)
    {
        uint16Put(offset() + DATA_OFFSET_FIELD_OFFSET, (short)dataOffset, LITTLE_ENDIAN);
        return this;
    }

}
