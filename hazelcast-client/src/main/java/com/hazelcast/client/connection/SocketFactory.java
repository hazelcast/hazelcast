package com.hazelcast.client.connection;

import java.io.IOException;
import java.net.Socket;

/**
 * @author mdogan 8/23/13
 */
public interface SocketFactory {

    Socket createSocket() throws IOException;
}
