/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

    public SSLSocketFactory(Properties properties) {
        this.properties = properties != null ? properties : new Properties();
        sslContextFactory = new BasicSSLContextFactory();
    }

    public SSLSocketFactory(SSLContextFactory sslContextFactory, Properties properties) {
        if (sslContextFactory == null) {
            throw new NullPointerException("SSLContextFactory is required!");
        }
        this.sslContextFactory = sslContextFactory;
        this.properties = properties != null ? properties : new Properties();
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
