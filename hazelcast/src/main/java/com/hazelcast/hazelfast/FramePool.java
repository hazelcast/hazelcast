package com.hazelcast.hazelfast;

import java.util.ArrayDeque;

public final class FramePool {
    private final boolean enabled;
    private final ArrayDeque<Frame> deque;

    public FramePool(boolean enabled) {
        this.enabled = enabled;
        this.deque = enabled ? new ArrayDeque<>() : null;
    }

    public void returnToPool(Frame frame) {
        if (!enabled) return;

        frame.length = 0;
        frame.bytes = null;
        deque.add(frame);
    }

    public Frame takeFromPool() {
        if (!enabled) return new Frame();

        Frame frame = deque.poll();
        if (frame == null)
            frame = new Frame();

        return frame;
    }
}
