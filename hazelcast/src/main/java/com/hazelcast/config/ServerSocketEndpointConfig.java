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

import com.hazelcast.instance.ProtocolType;
import com.hazelcast.internal.util.StringUtil;
import com.hazelcast.spi.annotation.PrivateApi;

import java.util.Collection;
import java.util.Objects;

/**
 * Endpoint configuration that defines a listening side (server)
 *
 * @since 3.12
 */
public class ServerSocketEndpointConfig
        extends EndpointConfig {

    /**
     * Default value of port number.
     */
    public static final int DEFAULT_PORT = 5701;

    /**
     * Default port auto-increment count.
     */
    public static final int PORT_AUTO_INCREMENT = 100;

    private static final int PORT_MAX = 0xFFFF;

    private int port = DEFAULT_PORT;

    private int portCount = PORT_AUTO_INCREMENT;

    private boolean portAutoIncrement = true;

    private boolean reuseAddress;

    private String publicAddress;

    public ServerSocketEndpointConfig() {
        String os = StringUtil.lowerCaseInternal(System.getProperty("os.name"));
        reuseAddress = (!os.contains("win"));
    }

    public String getPublicAddress() {
        return publicAddress;
    }

    /**
     * Overrides the public address of a member.
     * Behind a NAT, two endpoints may not be able to see/access each other.
     * If both nodes set their public addresses to their defined addresses on NAT, then that way
     * they can communicate with each other.
     * It should be set in the format “host IP address:port number”.
     */
    public ServerSocketEndpointConfig setPublicAddress(String publicAddress) {
        this.publicAddress = publicAddress;
        return this;
    }

    /**
     * Returns the port the Hazelcast member will try to bind on.
     * A port number of 0 will let the system pick up an ephemeral port.
     *
     * @return the port the Hazelcast member will try to bind on
     * @see #setPort(int)
     */
    public int getPort() {
        return port;
    }

    /**
     * Sets the port the Hazelcast member will try to bind on.
     *
     * A valid port value is between 0 and 65535.
     * A port number of 0 will let the system pick up an ephemeral port.
     *
     * @param port the port the Hazelcast member will try to bind on
     * @return NetworkConfig the updated NetworkConfig
     * @see #getPort()
     * @see #setPortAutoIncrement(boolean) for more information
     */
    public ServerSocketEndpointConfig setPort(int port) {
        if (port < 0 || port > PORT_MAX) {
            throw new IllegalArgumentException("Port out of range: " + port + ". Allowed range [0,65535]");
        }
        this.port = port;
        return this;
    }

    /**
     * Returns the maximum number of ports allowed to try to bind on.
     *
     * @return the maximum number of ports allowed to try to bind on
     * @see #setPortCount(int)
     * @see #setPortAutoIncrement(boolean) for more information
     */
    public int getPortCount() {
        return portCount;
    }

    /**
     * The maximum number of ports allowed to use.
     *
     * @param portCount the maximum number of ports allowed to use
     * @see #setPortAutoIncrement(boolean) for more information
     * @return this configuration
     */
    public ServerSocketEndpointConfig setPortCount(int portCount) {
        if (portCount < 1) {
            throw new IllegalArgumentException("port count can't be smaller than 0");
        }
        this.portCount = portCount;
        return this;
    }

    /**
     * Checks if a Hazelcast member is allowed find a free port by incrementing the port number when it encounters
     * an occupied port.
     *
     * @return the portAutoIncrement
     * @see #setPortAutoIncrement(boolean)
     */
    public boolean isPortAutoIncrement() {
        return portAutoIncrement;
    }

    /**
     * Sets if a Hazelcast member is allowed to find a free port by incrementing the port number when it encounters
     * an occupied port.
     * <p>
     * If you explicitly want to control the port a Hazelcast member is going to use, you probably want to set
     * portAutoincrement to false. In this case, the Hazelcast member is going to try the port {@link #setPort(int)}
     * and if the port is not free, the member will not start and throw an exception.
     * <p>
     * If this value is set to true, Hazelcast will start at the port specified by {@link #setPort(int)} and will try
     * until it finds a free port, or until it runs out of ports to try {@link #setPortCount(int)}.
     *
     * @param portAutoIncrement the portAutoIncrement to set
     * @return the updated NetworkConfig
     * @see #isPortAutoIncrement()
     * @see #setPortCount(int)
     * @see #setPort(int)
     */
    public ServerSocketEndpointConfig setPortAutoIncrement(boolean portAutoIncrement) {
        this.portAutoIncrement = portAutoIncrement;
        return this;
    }

    public boolean isReuseAddress() {
        return reuseAddress;
    }

    /**
     * Sets the reuse address.
     * <p>
     * When should setReuseAddress(true) be used?
     * <p>
     * When the member is shutdown, the server socket port will be in TIME_WAIT state for the next 2 minutes or so. If you
     * start the member right after shutting it down, you may not be able to bind to the same port because it is in TIME_WAIT
     * state. if you set reuseAddress=true then TIME_WAIT will be ignored and you will be able to bind to the same port again.
     * <p>
     * This property should not be set to true on the Windows platform: see
     * <ol>
     * <li>http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6421091</li>
     * <li>http://www.hsc.fr/ressources/articles/win_net_srv/multiple_bindings.html</li>
     * </ol>
     * By default, if the OS is Windows then reuseAddress will be false.
     */
    public ServerSocketEndpointConfig setReuseAddress(boolean reuseAddress) {
        this.reuseAddress = reuseAddress;
        return this;
    }

    @PrivateApi
    @Override
    public ServerSocketEndpointConfig setProtocolType(ProtocolType protocolType) {
        super.setProtocolType(protocolType);
        return this;
    }

    @Override
    public ServerSocketEndpointConfig setName(String name) {
        super.setName(name);
        return this;
    }

    @Override
    public ServerSocketEndpointConfig setOutboundPortDefinitions(Collection<String> outboundPortDefs) {
        super.setOutboundPortDefinitions(outboundPortDefs);
        return this;
    }

    @Override
    public ServerSocketEndpointConfig setOutboundPorts(Collection<Integer> outboundPorts) {
        super.setOutboundPorts(outboundPorts);
        return this;
    }

    @Override
    public ServerSocketEndpointConfig setInterfaces(InterfacesConfig interfaces) {
        super.setInterfaces(interfaces);
        return this;
    }

    @Override
    public ServerSocketEndpointConfig setSocketBufferDirect(boolean socketBufferDirect) {
        super.setSocketBufferDirect(socketBufferDirect);
        return this;
    }

    @Override
    public ServerSocketEndpointConfig setSocketKeepAlive(boolean socketKeepAlive) {
        super.setSocketKeepAlive(socketKeepAlive);
        return this;
    }

    @Override
    public ServerSocketEndpointConfig setSocketTcpNoDelay(boolean socketTcpNoDelay) {
        super.setSocketTcpNoDelay(socketTcpNoDelay);
        return this;
    }

    @Override
    public ServerSocketEndpointConfig setSocketSendBufferSizeKb(int socketSendBufferSizeKb) {
        super.setSocketSendBufferSizeKb(socketSendBufferSizeKb);
        return this;
    }

    @Override
    public ServerSocketEndpointConfig setSocketRcvBufferSizeKb(int socketRcvBufferSizeKb) {
        super.setSocketRcvBufferSizeKb(socketRcvBufferSizeKb);
        return this;
    }

    @Override
    public EndpointConfig setSocketLingerSeconds(int socketLingerSeconds) {
        super.setSocketLingerSeconds(socketLingerSeconds);
        return this;
    }

    @Override
    public ServerSocketEndpointConfig setSocketConnectTimeoutSeconds(int socketConnectTimeoutSeconds) {
        super.setSocketConnectTimeoutSeconds(socketConnectTimeoutSeconds);
        return this;
    }

    @Override
    public ServerSocketEndpointConfig setSocketInterceptorConfig(SocketInterceptorConfig socketInterceptorConfig) {
        super.setSocketInterceptorConfig(socketInterceptorConfig);
        return this;
    }

    @Override
    public ServerSocketEndpointConfig setSSLConfig(SSLConfig sslConfig) {
        super.setSSLConfig(sslConfig);
        return this;
    }

    @Override
    public ServerSocketEndpointConfig setSymmetricEncryptionConfig(SymmetricEncryptionConfig symmetricEncryptionConfig) {
        super.setSymmetricEncryptionConfig(symmetricEncryptionConfig);
        return this;
    }

    @Override
    public String toString() {
        return "EndpointConfig{"
                + "protocolType=" + protocolType
                + ", name=" + name
                + ", port=" + port
                + ", portCount=" + portCount
                + ", portAutoIncrement=" + portAutoIncrement
                + ", publicAddress=" + publicAddress
                + ", interfaces=" + interfaces
                + ", sslConfig=" + sslConfig
                + ", socketInterceptorConfig=" + socketInterceptorConfig
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        ServerSocketEndpointConfig that = (ServerSocketEndpointConfig) o;
        return port == that.port && portCount == that.portCount && portAutoIncrement == that.portAutoIncrement
                && reuseAddress == that.reuseAddress && Objects.equals(publicAddress, that.publicAddress);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), port, portCount, portAutoIncrement, reuseAddress, publicAddress);
    }
}
