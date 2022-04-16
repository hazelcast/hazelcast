package com.hazelcast.spi.impl.engine.nio;

import java.nio.channels.SelectionKey;

public interface NioSelectedKeyListener {

    void handle(SelectionKey key);
}
