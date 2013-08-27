package com.hazelcast.client.connection;

import java.io.IOException;
import java.net.Socket;

/**
 * @author mdogan 8/23/13
 */
public class DefaultSocketFactory implements SocketFactory {

    public Socket createSocket() throws IOException {
        return new Socket();
    }
}
