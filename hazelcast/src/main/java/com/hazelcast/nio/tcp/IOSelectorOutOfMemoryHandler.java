package com.hazelcast.nio.tcp;

public interface IOSelectorOutOfMemoryHandler {

    void handle(OutOfMemoryError error);
}
