package com.hazelcast.internal.tpcengine.net;

import javax.net.ssl.SSLEngine;
import java.net.SocketAddress;
import java.util.Properties;

public interface SSLEngineFactory {

    /**
     * Initializes this class with config from {@link com.hazelcast.config.SSLConfig}
     *
     * @param properties properties form config
     * @param forClient if the SslEngineFactory is created for a client or for a member. This can be used to
     *                  validate the configuration.
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
     * @param peerAddress peer's {@link SocketAddress} - can be {@code null}. The address can be used for instance to a TLS hostname
     *        validation.
     * @return the created SSLEngine.
     */
    SSLEngine create(boolean clientMode, SocketAddress peerAddress);
}
