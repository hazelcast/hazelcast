/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.config;

import com.hazelcast.config.tpc.TpcSocketConfig;
import com.hazelcast.config.tpc.TpcConfig;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.instance.ProtocolType;
import com.hazelcast.internal.util.Preconditions;
import com.hazelcast.spi.annotation.Beta;
import com.hazelcast.spi.annotation.PrivateApi;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * Endpoint configuration that defines communication/networking properties common to both incoming/outgoing connections
 * e.g.
 * <p>
 * - Encryption / Security
 * - Hazelcast protocol type
 *
 * @since 3.12
 */
@SuppressWarnings("checkstyle:methodcount")
public class EndpointConfig implements NamedConfig {

    /**
     * See {@link java.net.SocketOptions#SO_TIMEOUT}
     */
    public static final int DEFAULT_SOCKET_CONNECT_TIMEOUT_SECONDS = 0;

    /**
     * See {@link java.net.SocketOptions#SO_SNDBUF}
     */
    public static final int DEFAULT_SOCKET_SEND_BUFFER_SIZE_KB = 128;

    /**
     * See {@link java.net.SocketOptions#SO_RCVBUF}.
     */
    public static final int DEFAULT_SOCKET_RECEIVE_BUFFER_SIZE_KB = 128;

    /**
     * See {@link java.net.SocketOptions#SO_LINGER}
     */
    public static final int DEFAULT_SOCKET_LINGER_SECONDS = 0;

    /**
     * See {@code jdk.net.ExtendedSocketOptions#TCP_KEEPIDLE}
     */
    public static final int DEFAULT_SOCKET_KEEP_IDLE_SECONDS = 7200;

    /**
     * See {@code jdk.net.ExtendedSocketOptions#TCP_KEEPINTERVAL}
     */
    public static final int DEFAULT_SOCKET_KEEP_INTERVAL_SECONDS = 75;

    /**
     * See {@code jdk.net.ExtendedSocketOptions#TCP_KEEPINTERVAL}
     */
    public static final int DEFAULT_SOCKET_KEEP_COUNT = 8;

    /**
     * Maximum configurable number of seconds for {@link #socketKeepIdleSeconds}
     */
    private static final int MAX_SOCKET_KEEP_IDLE_SECONDS = 32767;

    /**
     * Maximum configurable number of seconds for {@link #socketKeepIntervalSeconds}
     */
    private static final int MAX_SOCKET_KEEP_INTERVAL_SECONDS = 32767;

    /**
     * Maximum configurable number of keep alive probes for {@link #socketKeepCount}
     */
    private static final int MAX_SOCKET_KEEP_COUNT = 127;

    protected String name;
    protected ProtocolType protocolType;
    protected InterfacesConfig interfaces = new InterfacesConfig();
    protected SocketInterceptorConfig socketInterceptorConfig;
    protected SSLConfig sslConfig;
    protected SymmetricEncryptionConfig symmetricEncryptionConfig;
    private Collection<String> outboundPortDefinitions;
    private Collection<Integer> outboundPorts;
    private boolean socketBufferDirect;
    private boolean socketTcpNoDelay = true;
    private boolean socketKeepAlive = true;
    private int socketConnectTimeoutSeconds = DEFAULT_SOCKET_CONNECT_TIMEOUT_SECONDS;
    private int socketSendBufferSizeKb = DEFAULT_SOCKET_SEND_BUFFER_SIZE_KB;
    private int socketRcvBufferSizeKb = DEFAULT_SOCKET_RECEIVE_BUFFER_SIZE_KB;
    private int socketLingerSeconds = DEFAULT_SOCKET_LINGER_SECONDS;
    private int socketKeepIdleSeconds = DEFAULT_SOCKET_KEEP_IDLE_SECONDS;
    private int socketKeepIntervalSeconds = DEFAULT_SOCKET_KEEP_INTERVAL_SECONDS;
    private int socketKeepCount = DEFAULT_SOCKET_KEEP_COUNT;
    private TpcSocketConfig tpcSocketConfig = new TpcSocketConfig();

    public ProtocolType getProtocolType() {
        return protocolType;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public EndpointConfig setName(String name) {
        this.name = name;
        return this;
    }

    /**
     * Gets the {@link SymmetricEncryptionConfig}. The value can be {@code null} which means that no symmetric encryption should
     * be used.
     *
     * @deprecated
     *
     * @return the SymmetricEncryptionConfig
     */
    @Deprecated(since = "5.0")
    public SymmetricEncryptionConfig getSymmetricEncryptionConfig() {
        return symmetricEncryptionConfig;
    }

    /**
     * Sets the {@link SymmetricEncryptionConfig}. The value can be {@code null} if no symmetric encryption should be used.
     *
     * @deprecated
     *
     * @param symmetricEncryptionConfig the SymmetricEncryptionConfig to set
     * @return the updated NetworkConfig
     * @see #getSymmetricEncryptionConfig()
     */
    @Deprecated(since = "5.0")
    public EndpointConfig setSymmetricEncryptionConfig(SymmetricEncryptionConfig symmetricEncryptionConfig) {
        this.symmetricEncryptionConfig = symmetricEncryptionConfig;
        return this;
    }

    public EndpointQualifier getQualifier() {
        return EndpointQualifier.resolveForConfig(protocolType, name);
    }

    public Collection<String> getOutboundPortDefinitions() {
        return outboundPortDefinitions;
    }

    public EndpointConfig setOutboundPortDefinitions(final Collection<String> outboundPortDefs) {
        this.outboundPortDefinitions = outboundPortDefs;
        return this;
    }

    public EndpointConfig addOutboundPortDefinition(String portDef) {
        if (outboundPortDefinitions == null) {
            outboundPortDefinitions = new HashSet<>();
        }
        outboundPortDefinitions.add(portDef);
        return this;
    }

    public Collection<Integer> getOutboundPorts() {
        return outboundPorts;
    }

    public EndpointConfig setOutboundPorts(final Collection<Integer> outboundPorts) {
        this.outboundPorts = outboundPorts;
        return this;
    }

    public EndpointConfig addOutboundPort(int port) {
        if (outboundPorts == null) {
            outboundPorts = new HashSet<>();
        }
        outboundPorts.add(port);
        return this;
    }

    /**
     * @return the interfaces
     */
    public InterfacesConfig getInterfaces() {
        return interfaces;
    }

    /**
     * @param interfaces the interfaces to set
     */
    public EndpointConfig setInterfaces(final InterfacesConfig interfaces) {
        this.interfaces = interfaces;
        return this;
    }

    public boolean isSocketBufferDirect() {
        return socketBufferDirect;
    }

    public EndpointConfig setSocketBufferDirect(boolean socketBufferDirect) {
        this.socketBufferDirect = socketBufferDirect;
        return this;
    }

    public boolean isSocketTcpNoDelay() {
        return socketTcpNoDelay;
    }

    public boolean isSocketKeepAlive() {
        return socketKeepAlive;
    }

    public EndpointConfig setSocketKeepAlive(boolean socketKeepAlive) {
        this.socketKeepAlive = socketKeepAlive;
        return this;
    }

    public EndpointConfig setSocketTcpNoDelay(boolean socketTcpNoDelay) {
        this.socketTcpNoDelay = socketTcpNoDelay;
        return this;
    }

    public int getSocketSendBufferSizeKb() {
        return socketSendBufferSizeKb;
    }

    public EndpointConfig setSocketSendBufferSizeKb(int socketSendBufferSizeKb) {
        this.socketSendBufferSizeKb = socketSendBufferSizeKb;
        return this;
    }

    public int getSocketRcvBufferSizeKb() {
        return socketRcvBufferSizeKb;
    }

    public EndpointConfig setSocketRcvBufferSizeKb(int socketRcvBufferSizeKb) {
        this.socketRcvBufferSizeKb = socketRcvBufferSizeKb;
        return this;
    }

    public int getSocketLingerSeconds() {
        return socketLingerSeconds;
    }

    public EndpointConfig setSocketLingerSeconds(int socketLingerSeconds) {
        this.socketLingerSeconds = socketLingerSeconds;
        return this;
    }

    public int getSocketConnectTimeoutSeconds() {
        return socketConnectTimeoutSeconds;
    }

    public EndpointConfig setSocketConnectTimeoutSeconds(int socketConnectTimeoutSeconds) {
        this.socketConnectTimeoutSeconds = socketConnectTimeoutSeconds;
        return this;
    }

    /**
     * Gets the {@link SocketInterceptorConfig}. The value can be {@code null} if no socket interception is needed.
     *
     * @return the SocketInterceptorConfig
     * @see #setSocketInterceptorConfig(SocketInterceptorConfig)
     */
    public SocketInterceptorConfig getSocketInterceptorConfig() {
        return socketInterceptorConfig;
    }

    /**
     * Sets the {@link SocketInterceptorConfig}. The value can be {@code null} if no socket interception is needed.
     *
     * @param socketInterceptorConfig the SocketInterceptorConfig to set
     * @return the updated NetworkConfig
     */
    public EndpointConfig setSocketInterceptorConfig(SocketInterceptorConfig socketInterceptorConfig) {
        this.socketInterceptorConfig = socketInterceptorConfig;
        return this;
    }

    /**
     * Returns the current {@link SSLConfig}. It is possible that null is returned if no SSLConfig has been set.
     *
     * @return the SSLConfig
     * @see #setSSLConfig(SSLConfig)
     */
    public SSLConfig getSSLConfig() {
        return sslConfig;
    }

    /**
     * Sets the {@link SSLConfig}. null value indicates that no SSLConfig should be used.
     *
     * @param sslConfig the SSLConfig
     * @return the updated NetworkConfig
     * @see #getSSLConfig()
     */
    public EndpointConfig setSSLConfig(SSLConfig sslConfig) {
        this.sslConfig = sslConfig;
        return this;
    }

    /**
     * Gets the TpcSocketConfig. Can't return null.
     *
     * @return the TpcSocketConfig.
     * @see TpcConfig
     * @since 5.3
     */
    @Beta
    @Nonnull
    public TpcSocketConfig getTpcSocketConfig() {
        return tpcSocketConfig;
    }

    /**
     * Sets the TpcSocketConfig. Can't return null.
     *
     * @param tpcSocketConfig Tpc socket config to set
     * @return this endpoint config
     * @throws NullPointerException if tpcSocketConfig is null
     * @see TpcConfig
     * @since 5.3
     */
    @Beta
    @Nonnull
    public EndpointConfig setTpcSocketConfig(@Nonnull TpcSocketConfig tpcSocketConfig) {
        this.tpcSocketConfig = checkNotNull(tpcSocketConfig);
        return this;
    }

    @PrivateApi
    public EndpointConfig setProtocolType(ProtocolType protocolType) {
        this.protocolType = protocolType;
        return this;
    }

    /**
     * Keep-Alive idle time: the number of seconds of idle time before keep-alive initiates a probe.
     * This option is only applicable when {@link #setSocketKeepAlive(boolean) keep alive is true}.
     * Requires a recent JDK 8, JDK 11 or greater version that includes the required
     * <a href="https://bugs.openjdk.org/browse/JDK-8194298">JDK support</a>.
     *
     * @return the configured value of Keep-Alive idle time.
     * @since 5.3.0
     * @see <a href="https://docs.oracle.com/en/java/javase/11/docs/api/jdk.net/jdk/net/ExtendedSocketOptions.html#TCP_KEEPIDLE">
     *     jdk.net.ExtendedSocketOptions#TCP_KEEPIDLE</a>
     */
    public int getSocketKeepIdleSeconds() {
        return socketKeepIdleSeconds;
    }

    /**
     * Set the number of seconds a connection needs to be idle before TCP begins sending out keep-alive probes.
     * Valid values are 1 to 32767.
     * <p/>
     * This option is only applicable when {@link #setSocketKeepAlive(boolean) keep alive is true}.
     * Requires a recent JDK 8, JDK 11 or greater version that includes the required
     * <a href="https://bugs.openjdk.org/browse/JDK-8194298">JDK support</a>.
     *
     * @since 5.3.0
     * @see <a href="https://docs.oracle.com/en/java/javase/11/docs/api/jdk.net/jdk/net/ExtendedSocketOptions.html#TCP_KEEPIDLE">
     *      jdk.net.ExtendedSocketOptions#TCP_KEEPIDLE</a>
     */
    public EndpointConfig setSocketKeepIdleSeconds(int socketKeepIdleSeconds) {
        Preconditions.checkPositive("socketKeepIdleSeconds", socketKeepIdleSeconds);
        Preconditions.checkTrue(socketKeepIdleSeconds < MAX_SOCKET_KEEP_IDLE_SECONDS,
                "socketKeepIdleSeconds value " + socketKeepIdleSeconds + " is outside valid range 1 - 32767");
        this.socketKeepIdleSeconds = socketKeepIdleSeconds;
        return this;
    }

    /**
     * Keep-Alive interval: the number of seconds between keep-alive probes.
     * This option is only applicable when {@link #setSocketKeepAlive(boolean) keep alive is true}.
     * Requires a recent JDK 8, JDK 11 or greater version that includes the required
     * <a href="https://bugs.openjdk.org/browse/JDK-8194298">JDK support</a>.
     *
     * @return the configured value of Keep-Alive interval time.
     * @since 5.3.0
     * @see <a href=
     *      "https://docs.oracle.com/en/java/javase/11/docs/api/jdk.net/jdk/net/ExtendedSocketOptions.html#TCP_KEEPINTERVAL">
     *      jdk.net.ExtendedSocketOptions#TCP_KEEPINTERVAL</a>
     */
    public int getSocketKeepIntervalSeconds() {
        return socketKeepIntervalSeconds;
    }

    /**
     * Set the number of seconds between keep-alive probes. Notice that this is the number of seconds between probes after the
     * initial {@link #setSocketKeepIdleSeconds(int) keep-alive idle time} has passed. Valid values are 1 to 32767.
     * <p/>
     * This option is only applicable when {@link #setSocketKeepAlive(boolean) keep alive is true}. Requires a recent JDK 8, JDK
     * 11 or greater version that includes the required <a href="https://bugs.openjdk.org/browse/JDK-8194298">JDK support</a>.
     *
     * @since 5.3.0
     * @see <a href=
     *      "https://docs.oracle.com/en/java/javase/11/docs/api/jdk.net/jdk/net/ExtendedSocketOptions.html#TCP_KEEPINTERVAL">
     *      jdk.net.ExtendedSocketOptions#TCP_KEEPINTERVAL</a>
     */
    public EndpointConfig setSocketKeepIntervalSeconds(int socketKeepIntervalSeconds) {
        Preconditions.checkPositive("socketKeepIntervalSeconds", socketKeepIntervalSeconds);
        Preconditions.checkTrue(socketKeepIntervalSeconds < MAX_SOCKET_KEEP_INTERVAL_SECONDS,
                "socketKeepIntervalSeconds value " + socketKeepIntervalSeconds + " is outside valid range 1 - 32767");
        this.socketKeepIntervalSeconds = socketKeepIntervalSeconds;
        return this;
    }

    /**
     * Keep-Alive count: the maximum number of TCP keep-alive probes to send before giving up and closing the connection if no
     * response is obtained from the other side.
     * This option is only applicable when {@link #setSocketKeepAlive(boolean) keep alive is true}.
     * Requires a recent JDK 8, JDK 11 or greater version that includes the required
     * <a href="https://bugs.openjdk.org/browse/JDK-8194298">JDK support</a>.
     *
     * @return the configured value of Keep-Alive probe count.
     * @since 5.3.0
     * @see <a href="https://docs.oracle.com/en/java/javase/11/docs/api/jdk.net/jdk/net/ExtendedSocketOptions.html#TCP_KEEPCOUNT">
     *     jdk.net.ExtendedSocketOptions#TCP_KEEPCOUNT</a>
     */
    public int getSocketKeepCount() {
        return socketKeepCount;
    }

    /**
     * Set the maximum number of TCP keep-alive probes to send before giving up and closing the connection if no
     * response is obtained from the other side. Valid values are 1 to 127.
     * <p/>
     * This option is only applicable when {@link #setSocketKeepAlive(boolean) keep alive is true}.
     * Requires a recent JDK 8, JDK 11 or greater version that includes the required
     * <a href="https://bugs.openjdk.org/browse/JDK-8194298">JDK support</a>.
     *
     * @since 5.3.0
     * @see <a href="https://docs.oracle.com/en/java/javase/11/docs/api/jdk.net/jdk/net/ExtendedSocketOptions.html#TCP_KEEPCOUNT">
     *     jdk.net.ExtendedSocketOptions#TCP_KEEPCOUNT</a>
     */
    public EndpointConfig setSocketKeepCount(int socketKeepCount) {
        Preconditions.checkPositive("socketKeepCount", socketKeepCount);
        Preconditions.checkTrue(socketKeepCount < MAX_SOCKET_KEEP_COUNT,
                "socketKeepCount value " + socketKeepCount + " is outside valid range 1 - 127");
        this.socketKeepCount = socketKeepCount;
        return this;
    }

    @Override
    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        EndpointConfig that = (EndpointConfig) o;
        return socketBufferDirect == that.socketBufferDirect && socketTcpNoDelay == that.socketTcpNoDelay
                && socketKeepAlive == that.socketKeepAlive && socketConnectTimeoutSeconds == that.socketConnectTimeoutSeconds
                && socketSendBufferSizeKb == that.socketSendBufferSizeKb && socketRcvBufferSizeKb == that.socketRcvBufferSizeKb
                && socketKeepCount == that.socketKeepCount && socketKeepIdleSeconds == that.socketKeepIdleSeconds
                && socketKeepIntervalSeconds == that.socketKeepIntervalSeconds
                && socketLingerSeconds == that.socketLingerSeconds && Objects.equals(name, that.name)
                && protocolType == that.protocolType && Objects.equals(interfaces, that.interfaces)
                && Objects.equals(socketInterceptorConfig, that.socketInterceptorConfig)
                && Objects.equals(sslConfig, that.sslConfig)
                && Objects.equals(symmetricEncryptionConfig, that.symmetricEncryptionConfig)
                && Objects.equals(outboundPortDefinitions, that.outboundPortDefinitions)
                && Objects.equals(outboundPorts, that.outboundPorts)
                && Objects.equals(tpcSocketConfig, that.tpcSocketConfig);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, protocolType, interfaces, socketInterceptorConfig, sslConfig, symmetricEncryptionConfig,
                outboundPortDefinitions, outboundPorts, socketBufferDirect, socketTcpNoDelay, socketKeepAlive,
                socketConnectTimeoutSeconds, socketSendBufferSizeKb, socketRcvBufferSizeKb, socketLingerSeconds,
                tpcSocketConfig, socketKeepCount, socketKeepIdleSeconds, socketKeepIntervalSeconds);
    }
}
