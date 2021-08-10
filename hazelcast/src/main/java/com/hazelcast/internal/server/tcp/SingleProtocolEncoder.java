package com.hazelcast.internal.server.tcp;

import com.hazelcast.instance.ProtocolType;
import com.hazelcast.internal.networking.HandlerStatus;
import com.hazelcast.internal.networking.OutboundHandler;

import java.nio.ByteBuffer;

import static com.hazelcast.internal.networking.HandlerStatus.CLEAN;
import static com.hazelcast.internal.networking.HandlerStatus.DIRTY;
import static com.hazelcast.internal.nio.IOUtil.compactOrClear;
import static com.hazelcast.internal.nio.Protocols.PROTOCOL_LENGTH;
import static com.hazelcast.internal.util.StringUtil.stringToBytes;

public class SingleProtocolEncoder extends OutboundHandler<Void, ByteBuffer> {
    public final OutboundHandler[] outboundHandlers;

    private boolean isDecoderReceivedProtocol = false;
    private boolean isDecoderVerifiedProtocol = true;
    private boolean clusterProtocolBuffered = false;

    public SingleProtocolEncoder(OutboundHandler next) {
        this(new OutboundHandler[]{next});
    }

    public SingleProtocolEncoder(OutboundHandler[] next) {
        this.outboundHandlers = next;
    }

    @Override
    public HandlerStatus onWrite() throws Exception {
        compactOrClear(dst);

        try {
            // First, decoder must receive the protocol
            if (!isDecoderReceivedProtocol && !channel.isClientMode()) {
                return CLEAN;
            }

            // Decoder didn't verify the protocol, protocol error should be sent
            if (!isDecoderVerifiedProtocol) {
                if (!sendProtocol()) {
                    return DIRTY;
                }
            }

            // Set up the next encoder in the pipeline
            setupNextEncoder();

            return CLEAN;
        } finally {
            dst.flip();
        }
    }

    private boolean sendProtocol() {
        if (!clusterProtocolBuffered) {
            clusterProtocolBuffered = true;
            dst.put(stringToBytes("HZX"));
            return false;
        }

        return isProtocolBufferDrained();
    }

    protected void setupNextEncoder() {
        channel.outboundPipeline().replace(this, outboundHandlers);
    }

    @Override
    public void handlerAdded() {
        initDstBuffer(PROTOCOL_LENGTH);
    }

    private boolean isProtocolBufferDrained() {
        return dst.position() == 0;
    }

    public void signalProtocolVerified() {
        isDecoderReceivedProtocol = true;
        channel.outboundPipeline().wakeup();
    }

    public void signalWrongProtocol() {
        isDecoderReceivedProtocol = true;
        isDecoderVerifiedProtocol = false;
        channel.outboundPipeline().wakeup();
    }
}
