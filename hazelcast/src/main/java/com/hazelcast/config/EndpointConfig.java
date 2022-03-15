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

package com.hazelcast.config;

import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.instance.ProtocolType;
import com.hazelcast.spi.annotation.PrivateApi;

import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;

/**
 * Endpoint configuration that defines communication/networking properties common to both incoming/outgoing connections
 * eg.
 *
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

    public ProtocolType getProtocolType() {
        return protocolType;
    }

    public String getName() {
        return name;
    }

    public EndpointConfig setName(String name) {
        this.name = name;
        return this;
    }

    /**
     * Gets the {@link SymmetricEncryptionConfig}. The value can be {@code null} which means that no symmetric encryption should
     * be used.
     *
     * @deprecated since 4.2
     *
     * @return the SymmetricEncryptionConfig
     */
    @Deprecated
    public SymmetricEncryptionConfig getSymmetricEncryptionConfig() {
        return symmetricEncryptionConfig;
    }

    /**
     * Sets the {@link SymmetricEncryptionConfig}. The value can be {@code null} if no symmetric encryption should be used.
     *
     * @deprecated since 4.2
     *
     * @param symmetricEncryptionConfig the SymmetricEncryptionConfig to set
     * @return the updated NetworkConfig
     * @see #getSymmetricEncryptionConfig()
     */
    @Deprecated
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
            outboundPortDefinitions = new HashSet<String>();
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
            outboundPorts = new HashSet<Integer>();
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

    @PrivateApi
    public EndpointConfig setProtocolType(ProtocolType protocolType) {
        this.protocolType = protocolType;
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
                && socketLingerSeconds == that.socketLingerSeconds && Objects.equals(name, that.name)
                && protocolType == that.protocolType && Objects.equals(interfaces, that.interfaces)
                && Objects.equals(socketInterceptorConfig, that.socketInterceptorConfig)
                && Objects.equals(sslConfig, that.sslConfig)
                && Objects.equals(symmetricEncryptionConfig, that.symmetricEncryptionConfig)
                && Objects.equals(outboundPortDefinitions, that.outboundPortDefinitions)
                && Objects.equals(outboundPorts, that.outboundPorts);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, protocolType, interfaces, socketInterceptorConfig, sslConfig, symmetricEncryptionConfig,
                outboundPortDefinitions, outboundPorts, socketBufferDirect, socketTcpNoDelay, socketKeepAlive,
                socketConnectTimeoutSeconds, socketSendBufferSizeKb, socketRcvBufferSizeKb, socketLingerSeconds);
    }
}
