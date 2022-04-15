package com.hazelcast.spi.impl.engine.nio;

import java.nio.channels.SelectionKey;

public interface NioSelectionListener {

    void handle(SelectionKey key);
}
