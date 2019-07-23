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

import java.util.Arrays;
import java.util.LinkedList;
import java.util.ListIterator;

/**
 * <p>
 * Client Message is the carrier framed data as defined below.
 * </p>
 * <p>
 * Any request parameter, response or event data will be carried in
 * the payload.
 * </p>
 * <pre>
 * 0                   1                   2                   3
 * <p>
 * Client Message is the carrier framed data as defined below.
 * </p>
 * <p>
 * Any request parameter, response or event data will be carried in
 * the payload.
 * </p>
 * <p/>
 * <pre>
 *
 * request         = frame-length flags message-type correlation-id *1partition-id *fix-sized-param *var-sized-param
 * response        = normal-response | event-response | error
 * normal-response = frame-length flags  correlation-id *fix-sized-param *var-sized-param
 * event-response  = frame-length flags correlation-id  partition-id *fix-sized-param *var-sized-param
 *
 * ;current structure of error design is subject to change because of missing features (nested cause, missing ability for user exceptions).
 *
 * error               = frame-length error-response-flags correlationId error-code cause-error-code class-name cause-class-name message *stack-trace-element
 * error-code          = int32
 * cause-error-code    = int32
 * class-name          = utf8
 * message             = utf8
 * cause-class-name    = utf8
 * stack-trace-element = line-number class-name method-name *1file-name
 * method-name         = utf8
 * file-name           = utf8
 * line-number         = int32
 *
 * client-message               = message-first-frame *var-sized-param
 * message-first-frame          = frame-length flags message-type correlation-id *fix-sized-param
 * first-frame-flags            = %b1 %b1 %b0 13unused ; begin-fragment:1 end-fragment:1 final:0 ......
 * frame-length                 = int32
 * message-type                 = int16
 * correlation-id               = int64
 *
 * var-sized-param              = string-frame / custom-type-frames / list-frames / map-frames
 * map-frames                   = list-frames list-frames                                       ; key-frames value-frames
 * list-frames                  = var-sized-param-list-frames | fixed-sized-param-list-frame
 *
 *
 * string-frame                 = frame-length flags *OCTET ; Contains UTF-8 encoded octets
 *
 * custom-type-frames           = begin-frame *1custom-type-first-frame *var-sized-param end-frame
 * custom-type-first-frame      = frame-length flags *fix-sized-param
 *
 * var-sized-param-list-frames  = begin-frame *var-sized-param  end-frame  ; all elements should be same type
 * fixed-sized-param-list-frame = frame-length flags *fixed-sized-param    ; all elements should be same type
 *
 * begin-frame                  = %x00 %x00 %x00 %x05 begin-flags                               ; frame-length is always 5
 * begin-flags                  = begin-fragment end-fragment final %b1 %b0 is-null 10reserved  ; begin data structure: 1, end data structure: 0
 * end-frame                    = %x00 %x00 %x00 %x05 end-flags                                 ; frame-length is always 5
 * end-flags                    = begin-fragment end-fragment final %b1 %b0 is-null 10reserved  ; next:0 or 1, begin list: 0, end list: 1
 *
 * flags          = begin-fragment end-fragment final begin-data-structure end-data-structure is-null is-event 9reserverd
 * begin-fragment = BIT ; reserved for fragmentation
 * end-fragment   = BIT ; reserved for fragmentation
 * final          = BIT ; set to 1 when this frame is the last frame of the client-message
 * is-null        = BIT ; set to 1 if this frame represents a null field.
 * begin          = BIT ; set to 1 if this is a begin-frame. begin-frame represents begin of a custom-type or a variable-field-list, 0 otherwise
 * end            = BIT ; set to 1 if this an end-frame. end-frame represents end of a custom-type or a variable-field-list, 0 otherwise
 * reserved       = BIT ; Reserved for future usage.
 * unused         = BIT ; Irrelevant int this context
 * is-event       = BIT ;
 *
 * fixed-sized-param        = *OCTET
 * ;fixed-sized-param       = OCTET / boolean / int16 / int32 / int64 / UUID
 * ;boolean                 = %x00 / %x01
 * ;int16                   = 16BIT
 * ;int32                   = 32BIT
 * ;int64                   = 64BIT
 * ;UUID                    = int64 int64
 * </pre>
 */
public class ClientMessage implements OutboundFrame, Iterable<ClientMessage.Frame> {

    public static final int TYPE_FIELD_OFFSET = 0;
    public static final int CORRELATION_ID_FIELD_OFFSET = TYPE_FIELD_OFFSET + Bits.SHORT_SIZE_IN_BYTES;

    //offset valid for fragmentation frames only
    public static final int FRAGMENTATION_ID_OFFSET = 0;

    //optional fixed partition id field offset
    public static final int PARTITION_ID_FIELD_OFFSET = CORRELATION_ID_FIELD_OFFSET + Bits.LONG_SIZE_IN_BYTES;

    // 1 1 1 0 1 0 ....
    // 0 1 1 0 1 0 ....
    // 1 1 0 0 0 0
    public static final int DEFAULT_FLAGS = 0;
    public static final int BEGIN_FRAGMENT = 1 << 15;
    public static final int END_FRAGMENT = 1 << 14;
    public static final int UNFRAGMENTED_MESSAGE = BEGIN_FRAGMENT | END_FRAGMENT;
    public static final int FINAL = 1 << 13;
    public static final int BEGIN_DATA_STRUCTURE = 1 << 12;
    public static final int END_DATA_STRUCTURE = 1 << 11;
    public static final int IS_NULL = 1 << 10;
    public static final int IS_EVENT = 1 << 9;

    //frame length + flags
    public static final int SIZE_OF_FRAMELENGHT_AND_FLAGS = Bits.INT_SIZE_IN_BYTES + Bits.SHORT_SIZE_IN_BYTES;
    public static Frame NULL_FRAME = new Frame(null, IS_NULL);
    public static Frame BEGIN_FRAME = new Frame(null, BEGIN_DATA_STRUCTURE);
    public static Frame END_FRAME = new Frame(null, END_DATA_STRUCTURE);

    public LinkedList<Frame> getFrames() {
        return frames;
    }

    public static class Frame {
        public byte[] content;
        //begin-fragment end-fragment final begin-data-structure end-data-structure is-null is-event 9reserverd
        public int flags;

        public Frame(byte[] content, int flags) {
            this.content = content;
            this.flags = flags;
        }

        public Frame copy() {
            byte[] newContent = Arrays.copyOf(content, content.length);
            return new Frame(newContent, flags);
        }

        public boolean isDataStructureEndFrame() {
            return ClientMessage.isFlagSet(flags, END_DATA_STRUCTURE);
        }

        public boolean isNullFrame() {
            return ClientMessage.isFlagSet(flags, IS_NULL);
        }

        public int getSize() {
            if (content == null) {
                return SIZE_OF_FRAMELENGHT_AND_FLAGS;
            } else {
                return SIZE_OF_FRAMELENGHT_AND_FLAGS + content.length;
            }
        }
    }

    LinkedList<Frame> frames;

    private transient boolean isRetryable;
    private transient boolean acquiresResource;
    private transient String operationName;
    private transient Connection connection;


    private ClientMessage() {

    }

    public static ClientMessage createForEncode() {
        ClientMessage clientMessage = new ClientMessage();
        clientMessage.frames = new LinkedList<>();
        return clientMessage;
    }

    public static ClientMessage createForDecode(LinkedList<Frame> frames) {
        ClientMessage clientMessage = new ClientMessage();
        clientMessage.frames = frames;
        return clientMessage;
    }

    public short getMessageType() {
        return Bits.readShortL(frames.get(0).content, ClientMessage.TYPE_FIELD_OFFSET);
    }

    public ClientMessage setMessageType(short messageType) {
        Bits.writeShortL(frames.get(0).content, TYPE_FIELD_OFFSET, messageType);
        return this;
    }

    public long getCorrelationId() {
        return Bits.readLongL(frames.get(0).content, CORRELATION_ID_FIELD_OFFSET);
    }

    public ClientMessage setCorrelationId(long correlationId) {
        Bits.writeLongL(frames.get(0).content, CORRELATION_ID_FIELD_OFFSET, correlationId);
        return this;
    }

    public int getPartitionId() {
        return Bits.readIntL(frames.get(0).content, PARTITION_ID_FIELD_OFFSET);
    }

    public ClientMessage setPartitionId(int partitionId) {
        Bits.writeIntL(frames.get(0).content, PARTITION_ID_FIELD_OFFSET, partitionId);
        return this;
    }

    public ListIterator<Frame> iterator() {
        return frames.listIterator();
    }

    public void addFrame(Frame frame) {
        frames.add(frame);
    }

    public int getHeaderFlags() {
        return frames.get(0).flags;
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
        for (Frame frame : frames) {
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
//        sb.append("connection=").append(connection);
        sb.append(", length=").append(getFrameLength());
//        sb.append(", correlationId=").append(getCorrelationId());
//        sb.append(", operation=").append(operationName);
//        sb.append(", messageType=").append(Integer.toHexString(getMessageType()));
//        sb.append(", isRetryable=").append(isRetryable());
//        sb.append(", isEvent=").append(isFlagSet(frames.get(0).flags, IS_EVENT));
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
        ClientMessage newMessage = new ClientMessage();
        newMessage.frames = new LinkedList<>(frames);

        Frame initialFrameCopy = newMessage.frames.get(0).copy();
        newMessage.frames.set(0, initialFrameCopy);

        newMessage.setCorrelationId(correlationId);

        newMessage.isRetryable = isRetryable;
        newMessage.acquiresResource = acquiresResource;
        newMessage.operationName = operationName;

        return newMessage;
    }

}
