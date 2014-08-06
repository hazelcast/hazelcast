package com.hazelcast.nio.tcp;

public interface OutOfMemoryPolicy {

    void handle(OutOfMemoryError error);
}
