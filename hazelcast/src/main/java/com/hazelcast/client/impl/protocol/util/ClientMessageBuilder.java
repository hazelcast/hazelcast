package com.hazelcast.client.impl.protocol.util;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.nio.Connection;

import java.nio.ByteBuffer;

import static com.hazelcast.client.impl.protocol.ClientMessage.BEGIN_AND_END_FLAGS;
import static com.hazelcast.client.impl.protocol.ClientMessage.BEGIN_FLAG;
import static com.hazelcast.client.impl.protocol.ClientMessage.END_FLAG;

/**
 * Builds {@link ClientMessage}s from byte chunks. Fragmented messages are merged into single messages before processed.
 */
public class ClientMessageBuilder {

    public interface MessageDelegator {

        void delegate(ClientMessage message, Connection connection);
    }


    private final Int2ObjectHashMap<BufferBuilder> builderBySessionIdMap = new Int2ObjectHashMap<BufferBuilder>();

    private final MessageDelegator delegator;
    private final Connection connection;

    private ClientMessage message = ClientMessage.create();

    public ClientMessageBuilder(Connection connection, MessageDelegator delegator) {
        this.connection = connection;
        this.delegator = delegator;
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
            } else {
                if (message.isFlagSet(BEGIN_FLAG)) {
                    final BufferBuilder builder = builderBySessionIdMap
                            .getOrDefault(message.getCorrelationId(), new Int2ObjectHashMap.Supplier<BufferBuilder>() {
                                @Override
                                public BufferBuilder get() {
                                    return new BufferBuilder();
                                }
                            });
                    builder.reset().append(message.buffer(), 0, message.getFrameLength());
                } else {
                    final BufferBuilder builder = builderBySessionIdMap.get(message.getCorrelationId());
                    if (null != builder && builder.limit() != 0) {
                        builder.append(message.buffer(), message.getDataOffset(), message.getFrameLength() - message.getDataOffset());

                        if (message.isFlagSet(END_FLAG)) {
                            final int msgLength = builder.limit();
                            ClientMessage cm = ClientMessage.createForDecode(ByteBuffer.wrap(builder.buffer().byteArray()), 0);
                            cm.setFrameLength(msgLength);
                            //HANDLE-MESSAGE
                            handleMessage(cm);
                            builder.reset().compact();
                        }
                    }
                }
            }
        }
    }

    protected void handleMessage(ClientMessage message) {
        message.index(message.getDataOffset() + message.offset());
        delegator.delegate(message, connection);
    }

}
