package com.hazelcast.client.impl.protocol.util;

import com.hazelcast.client.impl.protocol.ClientMessage;

import java.nio.ByteBuffer;

/**
 * ClientMessage builder from incoming bytebuffer parts
 */
public class ClientMessageAccumulator
        extends ClientMessage {

    public int accumulate(ByteBuffer byteBuffer) {
        int total = 0;
        if (index() == 0) {
            total += initFrameSize(byteBuffer);
        }
        while (index() >= BitUtil.SIZE_OF_INT && byteBuffer.hasRemaining() && !isComplete()) {
            total += accumulate(byteBuffer, remainingSize());
        }
        return total;
    }

    private int initFrameSize(ByteBuffer byteBuffer) {
        if (byteBuffer.remaining() < BitUtil.SIZE_OF_INT) {
            return 0;
        }
        final int accumulatedBytesSize = accumulate(byteBuffer, BitUtil.SIZE_OF_INT);
        return accumulatedBytesSize;
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

    public boolean isComplete() {
        return (index() > HEADER_SIZE) && (index() == getFrameLength());
    }

    public int remainingSize() {
        return getFrameLength() - index();
    }

    public ByteBuffer accumulatedByteBuffer() {
        final ByteBuffer byteBuffer = buffer().byteBuffer();
        if (byteBuffer != null) {
            byteBuffer.limit(index());
            byteBuffer.position(index());
            byteBuffer.flip();
            return byteBuffer;
        }
        return null;
    }
}
