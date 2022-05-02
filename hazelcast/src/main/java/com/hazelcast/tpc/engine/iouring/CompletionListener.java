package com.hazelcast.tpc.engine.iouring;

public interface CompletionListener {

    void handle(int fd, int res, int flags, byte op, short data);
}
