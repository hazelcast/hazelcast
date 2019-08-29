/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.networking.OutboundFrame;
import com.hazelcast.nio.Bits;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.serialization.BinaryInterface;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.Objects;

/**
 * Client Message is the carrier framed data as defined below.
 * Any request parameter, response or event data will be carried in
 * the payload.
 *
 * client-message               = message-first-frame *var-sized-param
 * message-first-frame          = frame-length flags message-type correlation-id *fix-sized-param
 * first-frame-flags            = %b1 %b1 %b0 13unused ; begin-fragment:1 end-fragment:1 final:0 ......
 * frame-length                 = int32
 * message-type                 = int32
 * correlation-id               = int64
 *
 * var-sized-param              = string-frame / custom-type-frames / var-sized-param-list-frames / fixed-sized-param-list-frame
 * / map-fixed-to-fixed-frame / map-var-sized-to-var-sized-frames / null-frame
 *
 * map-fixed-to-fixed-frame          = frame-length flags *fixed-size-entry
 * fixed-size-entry                  = fixed-sized-param fixed-sized-param
 * map-var-sized-to-var-sized-frames = begin-frame *var-sized-entry end-frame
 * var-sized-entry                   = var-sized-param var-sized-param
 *
 * //map-fixed-sized-to-var-sized-frames // Not defined yet. Has no usage yet.
 * //map-var-sized-to-fixed-sized-frames // Not defined yet. Has no usage yet.
 *
 * list-frames                  = var-sized-param-list-frames | fixed-sized-param-list-frame
 * var-sized-param-list-frames  = begin-frame *var-sized-param  end-frame  ; all elements should be same type
 * fixed-sized-param-list-frame = frame-length flags *fixed-sized-param    ; all elements should be same type
 *
 *
 * string-frame                 = frame-length flags *OCTET ; Contains UTF-8 encoded octets
 *
 * custom-type-frames           = begin-frame *1custom-type-first-frame *var-sized-param end-frame
 * custom-type-first-frame      = frame-length flags *fix-sized-param
 *
 *
 * null-frame                   = %x00 %x00 %x00 %x05 null-flags
 * null-flags                   = %b0  %b0  %b0 %b0 %b0 %b1 10reserved  ; is-null: 1
 * ; frame-length is always 5
 * begin-frame                  = %x00 %x00 %x00 %x05 begin-flags
 * ; begin data structure: 1, end data structure: 0
 * begin-flags                  = begin-fragment end-fragment final %b1 %b0 is-null 10reserved
 * ; frame-length is always 5
 * end-frame                    = %x00 %x00 %x00 %x05 end-flags
 * ; next:0 or 1, begin list: 0, end list: 1
 * end-flags                    = begin-fragment end-fragment final %b1 %b0 is-null 10reserved
 *
 * flags          = begin-fragment end-fragment final begin-data-structure end-data-structure is-null is-event 9reserved
 * ; reserved for fragmentation
 * begin-fragment = BIT
 * ; reserved for fragmentation
 * end-fragment   = BIT
 * ; set to 1 when this frame is the last frame of the client-message
 * final          = BIT
 * ; set to 1 if this frame represents a null field.
 * is-null        = BIT
 * ; set to 1 if this is a begin-frame. begin-frame represents begin of a custom-type or a variable-field-list, 0 otherwise
 * begin          = BIT
 * ; set to 1 if this an end-frame. end-frame represents end of a custom-type or a variable-field-list, 0 otherwise
 * end            = BIT
 * ; Reserved for future usage.
 * reserved       = BIT
 * ; Irrelevant int this context
 * unused         = BIT
 * is-event       = BIT ;
 *
 * fixed-sized-param        = *OCTET
 * ;fixed-sized-param       = OCTET / boolean / int16 / int32 / int64 / UUID
 * ;boolean                 = %x00 / %x01
 * ;int16                   = 16BIT
 * ;int32                   = 32BIT
 * ;int64                   = 64BIT
 * ;UUID                    = int64 int64
 */
@SuppressWarnings("checkstyle:MagicNumber")
@BinaryInterface
public final class ClientMessage extends LinkedList<ClientMessage.Frame> implements OutboundFrame {

    // All offsets here are offset of frame.content byte[]
    // Note that frames have frame length and flags before this byte[] content
    public static final int TYPE_FIELD_OFFSET = 0;
    public static final int CORRELATION_ID_FIELD_OFFSET = TYPE_FIELD_OFFSET + Bits.INT_SIZE_IN_BYTES;
    //offset valid for fragmentation frames only
    public static final int FRAGMENTATION_ID_OFFSET = 0;
    //optional fixed partition id field offset
    public static final int PARTITION_ID_FIELD_OFFSET = CORRELATION_ID_FIELD_OFFSET + Bits.LONG_SIZE_IN_BYTES;

    public static final int DEFAULT_FLAGS = 0;
    public static final int BEGIN_FRAGMENT_FLAG = 1 << 15;
    public static final int END_FRAGMENT_FLAG = 1 << 14;
    public static final int UNFRAGMENTED_MESSAGE = BEGIN_FRAGMENT_FLAG | END_FRAGMENT_FLAG;
    public static final int IS_FINAL_FLAG = 1 << 13;
    public static final int BEGIN_DATA_STRUCTURE_FLAG = 1 << 12;
    public static final int END_DATA_STRUCTURE_FLAG = 1 << 11;
    public static final int IS_NULL_FLAG = 1 << 10;
    public static final int IS_EVENT_FLAG = 1 << 9;

    //frame length + flags
    public static final int SIZE_OF_FRAME_LENGTH_AND_FLAGS = Bits.INT_SIZE_IN_BYTES + Bits.SHORT_SIZE_IN_BYTES;
    public static final Frame NULL_FRAME = new Frame(new byte[0], IS_NULL_FLAG);
    public static final Frame BEGIN_FRAME = new Frame(new byte[0], BEGIN_DATA_STRUCTURE_FLAG);
    public static final Frame END_FRAME = new Frame(new byte[0], END_DATA_STRUCTURE_FLAG);

    private static final long serialVersionUID = 1L;

    private transient boolean isRetryable;
    private transient boolean acquiresResource;
    private transient String operationName;
    private transient Connection connection;

    private ClientMessage() {

    }

    private ClientMessage(LinkedList<Frame> frames) {
        super(frames);
    }

    public static ClientMessage createForEncode() {
        return new ClientMessage();
    }

    public static ClientMessage createForDecode(LinkedList<Frame> frames) {
        return new ClientMessage(frames);
    }

    public int getMessageType() {
        return Bits.readIntL(get(0).content, ClientMessage.TYPE_FIELD_OFFSET);
    }

    public ClientMessage setMessageType(int messageType) {
        Bits.writeIntL(get(0).content, TYPE_FIELD_OFFSET, messageType);
        return this;
    }

    public long getCorrelationId() {
        return Bits.readLongL(get(0).content, CORRELATION_ID_FIELD_OFFSET);
    }

    public ClientMessage setCorrelationId(long correlationId) {
        Bits.writeLongL(get(0).content, CORRELATION_ID_FIELD_OFFSET, correlationId);
        return this;
    }

    public int getPartitionId() {
        return Bits.readIntL(get(0).content, PARTITION_ID_FIELD_OFFSET);
    }

    public ClientMessage setPartitionId(int partitionId) {
        Bits.writeIntL(get(0).content, PARTITION_ID_FIELD_OFFSET, partitionId);
        return this;
    }

    public int getHeaderFlags() {
        return get(0).flags;
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

    public static boolean isFlagSet(int flags, int flagMask) {
        int i = flags & flagMask;
        return i == flagMask;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    public Connection getConnection() {
        return connection;
    }

    public int getFrameLength() {
        int frameLength = 0;
        for (Frame frame : this) {
            frameLength += frame.getSize();
        }
        return frameLength;
    }

    public boolean isUrgent() {
        return false;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClientMessage{");
        sb.append("connection=").append(connection);
        if (size() > 0) {
            sb.append(", length=").append(getFrameLength());
            sb.append(", correlationId=").append(getCorrelationId());
            sb.append(", operation=").append(getOperationName());
            sb.append(", messageType=").append(Integer.toHexString(getMessageType()));
            sb.append(", isRetryable=").append(isRetryable());
            sb.append(", isEvent=").append(isFlagSet(get(0).flags, IS_EVENT_FLAG));
            sb.append(", isFragmented=").append(!isFlagSet(get(0).flags, UNFRAGMENTED_MESSAGE));
        }
        sb.append('}');
        return sb.toString();
    }

    /**
     * Copies the clientMessage efficiently with correlation id
     * Only initialFrame is duplicated, rest of the frames are shared
     *
     * @param correlationId new id
     * @return the copy message
     */
    public ClientMessage copyWithNewCorrelationId(long correlationId) {
        ClientMessage newMessage = new ClientMessage(this);

        Frame initialFrameCopy = newMessage.get(0).copy();
        newMessage.set(0, initialFrameCopy);

        newMessage.setCorrelationId(correlationId);

        newMessage.isRetryable = isRetryable;
        newMessage.acquiresResource = acquiresResource;
        newMessage.operationName = operationName;

        return newMessage;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        ClientMessage message = (ClientMessage) o;

        if (isRetryable != message.isRetryable) {
            return false;
        }
        if (acquiresResource != message.acquiresResource) {
            return false;
        }
        if (!Objects.equals(operationName, message.operationName)) {
            return false;
        }
        return Objects.equals(connection, message.connection);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (isRetryable ? 1 : 0);
        result = 31 * result + (acquiresResource ? 1 : 0);
        result = 31 * result + (operationName != null ? operationName.hashCode() : 0);
        result = 31 * result + (connection != null ? connection.hashCode() : 0);
        return result;
    }

    @SuppressWarnings("checkstyle:VisibilityModifier")
    public static class Frame {
        public final byte[] content;
        //begin-fragment end-fragment final begin-data-structure end-data-structure is-null is-event 9reserverd
        public int flags;

        public Frame(byte[] content) {
            this(content, DEFAULT_FLAGS);
        }

        public Frame(byte[] content, int flags) {
            assert content != null;
            this.content = content;
            this.flags = flags;
        }

        public Frame copy() {
            byte[] newContent = Arrays.copyOf(content, content.length);
            return new Frame(newContent, flags);
        }

        public boolean isEndFrame() {
            return ClientMessage.isFlagSet(flags, END_DATA_STRUCTURE_FLAG);
        }

        public boolean isNullFrame() {
            return ClientMessage.isFlagSet(flags, IS_NULL_FLAG);
        }

        public int getSize() {
            if (content == null) {
                return SIZE_OF_FRAME_LENGTH_AND_FLAGS;
            } else {
                return SIZE_OF_FRAME_LENGTH_AND_FLAGS + content.length;
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Frame frame = (Frame) o;

            if (flags != frame.flags) {
                return false;
            }
            return Arrays.equals(content, frame.content);
        }

        @Override
        public int hashCode() {
            int result = Arrays.hashCode(content);
            result = 31 * result + flags;
            return result;
        }
    }

}
