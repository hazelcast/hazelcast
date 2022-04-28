package com.hazelcast.spi.impl.engine.iouring;

public interface CompletionListener {

    void handle(int fd, int res, int flags, byte op, short data);
}
