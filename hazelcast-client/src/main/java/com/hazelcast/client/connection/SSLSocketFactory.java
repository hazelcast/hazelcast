package com.hazelcast.client.connection;

import javax.net.ssl.SSLSocket;
import java.io.IOException;

/**
 * @author mdogan 8/23/13
 */
public class SSLSocketFactory implements SocketFactory {

    public SSLSocket createSocket() throws IOException {
        javax.net.ssl.SSLSocketFactory factory = (javax.net.ssl.SSLSocketFactory) javax.net.ssl.SSLSocketFactory.getDefault();
        return (SSLSocket) factory.createSocket();
    }
}
