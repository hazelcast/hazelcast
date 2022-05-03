package com.hazelcast.tpc.engine;

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
            throw new RuntimeException("Unrecognized eventloop type [" + s + ']');
        }
    }
}
