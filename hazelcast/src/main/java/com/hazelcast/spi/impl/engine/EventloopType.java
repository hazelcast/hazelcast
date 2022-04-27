package com.hazelcast.spi.impl.engine;

public enum EventloopType {

    NIO, EPOLL, IOURING;

    public static EventloopType fromString(String s) {
        if (s.equals("io_uring") || s.equals("iouring")) {
            return IOURING;
        } else if (s.equals("nio")) {
            return NIO;
        } else if (s.equals("epoll")) {
            return EPOLL;
        } else {
            throw new RuntimeException("Unrecognized reactor type [" + s + ']');
        }
    }
}
