package com.hazelcast.client.impl.protocol.util;

import com.hazelcast.client.impl.protocol.ClientMessage;

import java.nio.ByteBuffer;

import static com.hazelcast.client.impl.protocol.ClientMessage.BEGIN_AND_END_FLAGS;
import static com.hazelcast.client.impl.protocol.ClientMessage.BEGIN_FLAG;
import static com.hazelcast.client.impl.protocol.ClientMessage.END_FLAG;

/**
 * Builds {@link ClientMessage}s from byte chunks. Fragmented messages are merged into single messages before processed.
 */
public class ClientMessageBuilder {

    private final Int2ObjectHashMap<BufferBuilder> builderBySessionIdMap = new Int2ObjectHashMap<BufferBuilder>();

    private final MessageHandler delegate;
    private ClientMessage message = ClientMessage.create();

    public ClientMessageBuilder(MessageHandler delegate) {
        this.delegate = delegate;
    }

    public void onData(final ByteBuffer buffer) {
        while (buffer.hasRemaining()) {
            final boolean complete = message.readFrom(buffer);
            if (!complete) {
                return;
            }

            //MESSAGE IS COMPLETE HERE
            if (message.isFlagSet(BEGIN_AND_END_FLAGS)) {
                //HANDLE-MESSAGE
                handleMessage(message);
                message = ClientMessage.create();
                continue;
            }

            if (message.isFlagSet(BEGIN_FLAG)) {     // first fragment
                final BufferBuilder builder = new BufferBuilder();
                builderBySessionIdMap.put(message.getCorrelationId(), builder);
                builder.append(message.buffer(), 0, message.getFrameLength());
            } else {
                final BufferBuilder builder = builderBySessionIdMap.get(message.getCorrelationId());
                if (builder.position() == 0) {
                    throw new IllegalStateException();
                }

                builder.append(message.buffer(), message.getDataOffset(), message.getFrameLength() - message.getDataOffset());

                if (message.isFlagSet(END_FLAG)) {
                    final int msgLength = builder.position();
                    ClientMessage cm = ClientMessage.createForDecode(builder.buffer(), 0);
                    cm.setFrameLength(msgLength);
                    //HANDLE-MESSAGE
                    handleMessage(cm);
                    builderBySessionIdMap.remove(message.getCorrelationId());

                }
            }

            message = ClientMessage.create();
        }
    }

    private void handleMessage(ClientMessage message) {
        message.index(message.getDataOffset());
        delegate.handleMessage(message);
    }

    /**
     * Implementers will be responsible to delegate the constructed message
     */
    public interface MessageHandler {

        /**
         * Received message to be processed
         *
         * @param message the ClientMessage
         */
        void handleMessage(ClientMessage message);
    }


}
