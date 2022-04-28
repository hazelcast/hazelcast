package com.hazelcast.spi.impl.engine.iouring;

import com.hazelcast.spi.impl.engine.frame.Frame;
import io.netty.channel.unix.IovArray;

import java.nio.ByteBuffer;
import java.util.Queue;

// todo: instead of an 'array' we could use a ring so we don't need to copy to an earlier position
// TODO: This class assumes direct byte buffers. For future safety we should also allow for non direct
public final class IOVector {

    private final static int IOV_MAX = 1024;

    private final Frame[] frames = new Frame[IOV_MAX];
    private int size = 0;
    private long pending;

    public boolean isEmpty() {
        return size == 0;
    }

    public Frame get(int index) {
        return frames[index];
    }

    public void fill(Queue<Frame> queue) {
        int count = IOV_MAX - size;
        for (int k = 0; k < count; k++) {
            Frame frame = queue.poll();
            if (frame == null) {
                break;
            }
            ByteBuffer buffer = frame.byteBuffer();
            frames[size] = frame;
            size++;
            pending += buffer.remaining();
        }
    }

    public void fillIoArray(IovArray iovArray) {
        for (int k = 0; k < size; k++) {
            Frame frame = frames[k];
            ByteBuffer byteBuffer = frame.byteBuffer();
            iovArray.add(byteBuffer, byteBuffer.remaining());
        }
    }

    public boolean add(Frame frame) {
        if (size == IOV_MAX) {
            return false;
        } else {
            ByteBuffer buffer = frame.byteBuffer();
            frames[size] = frame;
            size++;
            pending += buffer.remaining();
            return true;
        }
    }

    public void compact(long written) {
        if (written == pending) {
            for (int k = 0; k < size; k++) {
                frames[k].release();
                frames[k] = null;
            }
            size = 0;
            pending = 0;
        } else {
            long w = written;
            int toIndex = 0;
            int cachedSize = size;
            for (int k = 0; k < cachedSize; k++) {
                Frame frame = frames[k];
                ByteBuffer byteBuffer = frame.byteBuffer();
                int bufferRemaining = byteBuffer.remaining();
                if (w < bufferRemaining) {
                    byteBuffer.position(byteBuffer.position() + (int) w);
                    if (k == 0) {
                        // the first one is not empty, we are done
                        break;
                    } else {
                        frames[toIndex] = frames[k];
                        frames[k] = null;
                        toIndex++;
                    }
                } else {
                    w -= bufferRemaining;
                    size--;
                    frame.release();
                    frames[k] = null;
                }
            }

            pending -= written;
        }
    }

    public int size() {
        return size;
    }
}
