/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.nio.ssl;

import javax.net.ssl.SSLEngine;

import com.hazelcast.cluster.Address;

import java.util.Properties;

/**
 * The {@link SSLEngineFactory} is responsible for creating an SSLEngine instance.
 *
 * @since 3.9
 */
public interface SSLEngineFactory {

    /**
     * Initializes this class with config from {@link com.hazelcast.config.SSLConfig}
     *
     * @param properties properties form config
     * @param forClient if the SslEngineFactory is created for a client ({@code true}) or for a member ({@code false}).
     *                  This can be used to validate the configuration.
     * @throws Exception if something goes wrong while initializing.
     */
    void init(Properties properties, boolean forClient) throws Exception;

    /**
     * Creates a SSLEngine.
     *
     * @param clientMode if the SSLEngine should be in client mode, or server-mode. See {@link SSLEngine#getUseClientMode()}. If
     *        this SSLEngineFactory is used by a java-client, then clientMode will always be true. But if it is created for a
     *        member, then the side of the socket that initiated the connection will be in 'clientMode' while the other one will
     *        be in 'serverMode'.
     * @param peerAddress peer's {@link Address} - can be {@code null}. The address can be used for instance to a TLS hostname
     *        validation.
     * @return the created SSLEngine.
     */
    SSLEngine create(boolean clientMode, Address peerAddress);
}
