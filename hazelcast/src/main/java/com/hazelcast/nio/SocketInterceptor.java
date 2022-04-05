/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.nio;

import java.io.IOException;
import java.net.Socket;
import java.util.Properties;

/**
 * An interface that provides the ability to intercept the creation of sockets.
 * It can be registered from client via config.
 *
 * For members see {@link MemberSocketInterceptor}
 *
 * Warning: a SocketInterceptor provides access to the socket and will bypass
 * any TLS encryption. So be warned that any data send using the SocketInterceptor
 * could be visible as plain text and could therefor be a security risk.
 *
 * see {@link com.hazelcast.config.SocketInterceptorConfig}
 */
public interface SocketInterceptor {

    /**
     * Initializes socket interceptor with properties which is set by
     * {@link com.hazelcast.config.Config#setProperty(String, String)}
     *
     * @param properties from hazelcast config
     */
    void init(Properties properties);

    /**
     * Called when a connection is established.
     *
     * @param connectedSocket related socket
     * @throws IOException in case of any exceptional case
     */
    void onConnect(Socket connectedSocket) throws IOException;
}
