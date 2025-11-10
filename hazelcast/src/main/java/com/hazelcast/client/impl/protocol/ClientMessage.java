/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.nio.Bits;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.tpcengine.net.AsyncSocket;

import java.util.Arrays;
import java.util.Objects;

/**
 * Client Message is the carrier framed data as defined below.
 * Any request parameter, response or event data will be carried in
 * the payload.
 *
 * <pre>
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
 * </pre>
 */
@SuppressWarnings("checkstyle:MagicNumber")
public final class ClientMessage implements OutboundFrame {

    // All offsets here are offset of frame.content byte[]
    // Note that frames have frame length and flags before this byte[] content
    public static final int TYPE_FIELD_OFFSET = 0;
    public static final int CORRELATION_ID_FIELD_OFFSET = TYPE_FIELD_OFFSET + Bits.INT_SIZE_IN_BYTES;
    //backup acks field offset is used by response messages
    public static final int RESPONSE_BACKUP_ACKS_FIELD_OFFSET = CORRELATION_ID_FIELD_OFFSET + Bits.LONG_SIZE_IN_BYTES;
    //partition id field offset used by request and event messages
    public static final int PARTITION_ID_FIELD_OFFSET = CORRELATION_ID_FIELD_OFFSET + Bits.LONG_SIZE_IN_BYTES;
    //offset valid for fragmentation frames only
    public static final int FRAGMENTATION_ID_OFFSET = 0;

    public static final int DEFAULT_FLAGS = 0;
    public static final int BEGIN_FRAGMENT_FLAG = 1 << 15;
    public static final int END_FRAGMENT_FLAG = 1 << 14;
    public static final int UNFRAGMENTED_MESSAGE = BEGIN_FRAGMENT_FLAG | END_FRAGMENT_FLAG;
    public static final int IS_FINAL_FLAG = 1 << 13;
    public static final int BEGIN_DATA_STRUCTURE_FLAG = 1 << 12;
    public static final int END_DATA_STRUCTURE_FLAG = 1 << 11;
    public static final int IS_NULL_FLAG = 1 << 10;
    public static final int IS_EVENT_FLAG = 1 << 9;
    public static final int BACKUP_AWARE_FLAG = 1 << 8;
    public static final int BACKUP_EVENT_FLAG = 1 << 7;

    //frame length + flags
    public static final int SIZE_OF_FRAME_LENGTH_AND_FLAGS = Bits.INT_SIZE_IN_BYTES + Bits.SHORT_SIZE_IN_BYTES;
    public static final Frame NULL_FRAME = new Frame(new byte[0], IS_NULL_FLAG);
    public static final Frame BEGIN_FRAME = new Frame(new byte[0], BEGIN_DATA_STRUCTURE_FLAG);
    public static final Frame END_FRAME = new Frame(new byte[0], END_DATA_STRUCTURE_FLAG);

    private static final long serialVersionUID = 1L;

    private transient Frame startFrame;
    private Frame endFrame;

    private transient boolean isRetryable;
    private transient String operationName;
    private transient Connection connection;
    private transient boolean containsSerializedDataInRequest;

    private AsyncSocket asyncSocket;

    private ClientMessage() {
    }

    //Constructs client message with single frame. StartFrame.next must be null.
    private ClientMessage(Frame startFrame) {
        assert startFrame.next == null;
        this.startFrame = startFrame;
        endFrame = startFrame;
    }

    public ClientMessage(Frame startFrame, Frame endFrame) {
        this.startFrame = startFrame;
        this.endFrame = endFrame;
    }

    public static ClientMessage createForEncode() {
        return new ClientMessage();
    }

    public static ClientMessage createForDecode(Frame startFrame) {
        return new ClientMessage(startFrame);
    }

    public Frame getStartFrame() {
        return startFrame;
    }

    public Frame getEndFrame() {
        return endFrame;
    }

    public ClientMessage add(Frame frame) {
        frame.next = null;
        if (startFrame == null) {
            startFrame = frame;
            endFrame = frame;
            return this;
        }

        endFrame.next = frame;
        endFrame = frame;
        return this;
    }

    public ForwardFrameIterator frameIterator() {
        return new ForwardFrameIterator(startFrame);
    }

    public int getMessageType() {
        return Bits.readIntL(startFrame.content, ClientMessage.TYPE_FIELD_OFFSET);
    }

    public ClientMessage setMessageType(int messageType) {
        Bits.writeIntL(startFrame.content, TYPE_FIELD_OFFSET, messageType);
        return this;
    }

    /**
     * This ID correlates the request to responses. It should be unique to identify
     * one message in the communication. This ID is used to track the request-response
     * cycle of a client operation. Members send response messages with the same ID as
     * the request message. The uniqueness is per connection. If the client receives
     * the response to a request and the request is not a multi-response
     * request (i.e. not a request for event transmission), then the correlation ID for
     * the request can be reused by the subsequent requests. Note that once a correlation
     * ID is used to register for an event, it SHOULD NOT be used again unless the client
     * unregisters (stops listening) for that event.
     *
     * @return the correlation id of the message
     */
    public long getCorrelationId() {
        return Bits.readLongL(startFrame.content, CORRELATION_ID_FIELD_OFFSET);
    }

    /**
     * Sets the correlation id of the message. This ID correlates the request to responses.
     * @param correlationId the correlation id of the message
     * @return the ClientMessage with the new correlation id
     */
    public ClientMessage setCorrelationId(long correlationId) {
        Bits.writeLongL(startFrame.content, CORRELATION_ID_FIELD_OFFSET, correlationId);
        return this;
    }

    /**
     * @return the number of acks will be send for a request
     */
    public byte getNumberOfBackupAcks() {
        return getStartFrame().content[RESPONSE_BACKUP_ACKS_FIELD_OFFSET];
    }

    /**
     * Sets the setNumberOfAcks field.
     *
     * @param numberOfAcks The value to set in the setNumberOfAcks field.
     * @return The ClientMessage with the new dataOffset field value.
     */
    public ClientMessage setNumberOfBackupAcks(final byte numberOfAcks) {
        getStartFrame().content[RESPONSE_BACKUP_ACKS_FIELD_OFFSET] = numberOfAcks;
        return this;
    }

    public int getPartitionId() {
        return Bits.readIntL(startFrame.content, PARTITION_ID_FIELD_OFFSET);
    }

    public ClientMessage setPartitionId(int partitionId) {
        Bits.writeIntL(startFrame.content, PARTITION_ID_FIELD_OFFSET, partitionId);
        return this;
    }

    public int getHeaderFlags() {
        return startFrame.flags;
    }

    public boolean isRetryable() {
        return isRetryable;
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

    public void setAsyncSocket(AsyncSocket asyncSocket) {
        this.asyncSocket = asyncSocket;
    }

    public AsyncSocket getAsyncSocket() {
        return asyncSocket;
    }

    public Connection getConnection() {
        return connection;
    }

    @Override
    public int getFrameLength() {
        int frameLength = 0;
        Frame currentFrame = startFrame;
        while (currentFrame != null) {
            frameLength += currentFrame.getSize();
            currentFrame = currentFrame.next;
        }
        return frameLength;
    }

    public int getBufferLength() {
        int length = 0;
        Frame currentFrame = startFrame;
        while (currentFrame != null) {
            length += currentFrame.content.length + SIZE_OF_FRAME_LENGTH_AND_FLAGS;
            currentFrame = currentFrame.next;

        }
        return length;
    }

    @Override
    public boolean isUrgent() {
        return false;
    }

    public void merge(ClientMessage fragment) {
        endFrame.next = fragment.startFrame;
        endFrame = fragment.endFrame;
    }

    public void dropFragmentationFrame() {
        startFrame = startFrame.next;
    }

    public boolean isContainsSerializedDataInRequest() {
        return containsSerializedDataInRequest;
    }

    public void setContainsSerializedDataInRequest(boolean containsSerializedDataInRequest) {
        this.containsSerializedDataInRequest = containsSerializedDataInRequest;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClientMessage{");
        sb.append("connection=").append(connection);
        if (startFrame != null) {
            sb.append(", length=").append(getFrameLength());
            sb.append(", operation=").append(getOperationName());
            sb.append(", isRetryable=").append(isRetryable());

            boolean beginFragment = isFlagSet(startFrame.flags, BEGIN_FRAGMENT_FLAG);
            boolean unFragmented = isFlagSet(startFrame.flags, UNFRAGMENTED_MESSAGE);
            // print correlation id, and message type only if it is unfragmented message or
            // the first message of a fragmented message
            if (unFragmented) {
                sb.append(", correlationId=").append(getCorrelationId());
                sb.append(", messageType=").append(Integer.toHexString(getMessageType()));
                sb.append(", isEvent=").append(isFlagSet(startFrame.flags, IS_EVENT_FLAG));
            } else if (beginFragment) {
                Frame messageFirstFrame = startFrame.next;
                sb.append(", fragmentationId=").append(Bits.readLongL(startFrame.content, FRAGMENTATION_ID_OFFSET));
                sb.append(", correlationId=").append(Bits.readLongL(messageFirstFrame.content, CORRELATION_ID_FIELD_OFFSET));
                sb.append(", messageType=")
                  .append(Integer.toHexString(Bits.readIntL(messageFirstFrame.content, ClientMessage.TYPE_FIELD_OFFSET)));
                sb.append(", isEvent=").append(isFlagSet(messageFirstFrame.flags, IS_EVENT_FLAG));
            } else {
                sb.append(", fragmentationId=").append(Bits.readLongL(startFrame.content, FRAGMENTATION_ID_OFFSET));
            }
            sb.append(", isfragmented=").append(!unFragmented);
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
        Frame initialFrameCopy = startFrame.deepCopy();
        ClientMessage newMessage = new ClientMessage(initialFrameCopy, endFrame);

        newMessage.setCorrelationId(correlationId);

        newMessage.isRetryable = isRetryable;
        newMessage.operationName = operationName;
        newMessage.containsSerializedDataInRequest = containsSerializedDataInRequest;

        return newMessage;
    }

    /**
     * Only deep copies the initial frame to not duplicate the rest of the
     * message to get rid of unnecessary allocations for the retry of the same
     * client message.
     * <p>
     * It is expected that the correlation id for the returned message is set
     * later.
     *
     * @return the copied message
     */
    public ClientMessage copyMessageWithSharedNonInitialFrames() {
        Frame initialFrameCopy = startFrame.deepCopy();
        ClientMessage newMessage = new ClientMessage(initialFrameCopy, endFrame);

        newMessage.isRetryable = isRetryable;
        newMessage.operationName = operationName;
        newMessage.containsSerializedDataInRequest = containsSerializedDataInRequest;

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
        if (containsSerializedDataInRequest != message.containsSerializedDataInRequest) {
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
        result = 31 * result + (containsSerializedDataInRequest ? 1 : 0);
        result = 31 * result + (operationName != null ? operationName.hashCode() : 0);
        result = 31 * result + (connection != null ? connection.hashCode() : 0);
        return result;
    }

    public static final class ForwardFrameIterator {
        private Frame nextFrame;

        private ForwardFrameIterator(Frame start) {
            nextFrame = start;
        }

        public Frame next() {
            Frame result = nextFrame;
            if (nextFrame != null) {
                nextFrame = nextFrame.next;
            }
            return result;
        }

        public boolean hasNext() {
            return nextFrame != null;
        }

        public Frame peekNext() {
            return nextFrame;
        }

    }

    @SuppressWarnings({"checkstyle:VisibilityModifier", "java:S1104"})
    public static class Frame {
        public final byte[] content;
        //begin-fragment end-fragment final begin-data-structure end-data-structure is-null is-event 9reserved
        public int flags;

        public Frame next;

        public Frame(byte[] content) {
            this(content, DEFAULT_FLAGS);
        }

        public Frame(byte[] content, int flags) {
            assert content != null;
            this.content = content;
            this.flags = flags;
        }

        // Shares the content bytes
        public Frame copy() {
            Frame frame = new Frame(content, flags);
            frame.next = this.next;
            return frame;
        }

        // Copies the content bytes
        public Frame deepCopy() {
            byte[] newContent = Arrays.copyOf(content, content.length);
            Frame frame = new Frame(newContent, flags);
            frame.next = this.next;
            return frame;
        }

        public boolean isEndFrame() {
            return ClientMessage.isFlagSet(flags, END_DATA_STRUCTURE_FLAG);
        }

        public boolean isBeginFrame() {
            return ClientMessage.isFlagSet(flags, BEGIN_DATA_STRUCTURE_FLAG);
        }

        public boolean isNullFrame() {
            return ClientMessage.isFlagSet(flags, IS_NULL_FLAG);
        }

        public int getSize() {
            return SIZE_OF_FRAME_LENGTH_AND_FLAGS + (content != null ? content.length : 0);
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
