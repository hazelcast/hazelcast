package com.hazelcast.tpc.engine.nio;

import java.io.IOException;
import java.nio.channels.SelectionKey;

public interface NioSelectedKeyListener {

    void handleException(Exception e);

    void handle(SelectionKey key) throws IOException;
}
