package io.netty.incubator.channel.uring;

public interface CompletionListener {

    void handle(int fd, int res, int flags, byte op, short data);
}
