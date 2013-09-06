package com.hazelcast.client.connection;

import com.hazelcast.nio.ssl.BasicSSLContextFactory;
import com.hazelcast.nio.ssl.SSLContextFactory;
import com.hazelcast.util.ExceptionUtil;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import java.io.IOException;
import java.util.Properties;

/**
 * @author mdogan 8/23/13
 */
// TODO: add SSLConfig to client config.
public class SSLSocketFactory implements SocketFactory {

    private final Properties properties;
    private final SSLContextFactory sslContextFactory;
    private volatile boolean initialized = false;

    public SSLSocketFactory() {
        sslContextFactory = new BasicSSLContextFactory();
        properties = new Properties();
    }

    public SSLSocketFactory(SSLContextFactory sslContextFactory, Properties properties) {
        this.sslContextFactory = sslContextFactory;
        this.properties = properties;
    }

    public SSLSocket createSocket() throws IOException {
        if (!initialized) {
            synchronized (this) {
                if (!initialized) {
                    try {
                        sslContextFactory.init(properties);
                    } catch (Exception e) {
                        throw ExceptionUtil.rethrow(e, IOException.class);
                    }
                    initialized = true;
                }
            }
        }
        SSLContext sslContext = sslContextFactory.getSSLContext();
        final javax.net.ssl.SSLSocketFactory factory = sslContext.getSocketFactory();
        return (SSLSocket) factory.createSocket();
    }
}
