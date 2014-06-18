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

package com.hazelcast.config;

import java.util.Collection;
import java.util.HashSet;

public class NetworkConfig {

    public static final int DEFAULT_PORT = 5701;

    private static final int PORT_AUTO_INCREMENT = 100;

    private int port = DEFAULT_PORT;

    private int portCount = PORT_AUTO_INCREMENT;

    private boolean portAutoIncrement = true;

    private boolean reuseAddress;

    private String publicAddress;

    private Collection<String> outboundPortDefinitions;

    private Collection<Integer> outboundPorts;

    private InterfacesConfig interfaces = new InterfacesConfig();

    private JoinConfig join = new JoinConfig();

    private SymmetricEncryptionConfig symmetricEncryptionConfig;

    private SocketInterceptorConfig socketInterceptorConfig;

    private SSLConfig sslConfig;

    public NetworkConfig() {
        String os = System.getProperty("os.name").toLowerCase();
        reuseAddress = (!os.contains("win"));
    }

    /**
     * Returns the port the Hazelcast member is going to try to bind on.
     *
     * @return the port the port to bind on.
     * @see #setPort(int)
     */
    public int getPort() {
        return port;
    }

    /**
     * Sets the port the Hazelcast member is going to try to bind on.
     *
     * @param port the port to bind on.
     * @return NetworkConfig the updated NetworkConfig
     * @see #getPort()
     * @see #setPortAutoIncrement(boolean) for more information.
     */
    public NetworkConfig setPort(int port) {
        this.port = port;
        return this;
    }

    /**
     * Returns the maximum number of ports allowed to try to bind on.
     *
     * @return the maximum number of ports allowed to try.
     * @see #setPortCount(int)
     * @see #setPortAutoIncrement(boolean) for more information.
     */
    public int getPortCount() {
        return portCount;
    }

    /**
     * The maxmim number of ports to try.
     *
     * @param portCount the maximum number of ports allowed to use.
     * @see #setPortAutoIncrement(boolean) for more information.
     */
    public void setPortCount(int portCount) {
        if (portCount < 1) {
            throw new IllegalArgumentException("port count can't be smaller than 0");
        }
        this.portCount = portCount;
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
     * <p/>
     * If you explicitly want to control the port the a Hazelcast member is going to use, you probably want to set
     * portAutoincrement to false. In this case, the Hazelcast member is going to try to the port {@link #setPort(int)}
     * and if the port is not free, the member will not start and throw an exception.
     * <p/>
     * If this value is set to true, Hazelcast will start at the port specified by {@link #setPort(int)} and will try
     * until it finds a free port, or until it runs out of ports to try {@link #setPortCount(int)}.
     *
     * @param portAutoIncrement the portAutoIncrement to set
     * @return the updated NetworkConfig
     * @see #isPortAutoIncrement()
     * @see #setPortCount(int)
     * @see #setPort(int)
     */
    public NetworkConfig setPortAutoIncrement(boolean portAutoIncrement) {
        this.portAutoIncrement = portAutoIncrement;
        return this;
    }

    public boolean isReuseAddress() {
        return reuseAddress;
    }

    public NetworkConfig setReuseAddress(boolean reuseAddress) {
        this.reuseAddress = reuseAddress;
        return this;
    }

    public Collection<String> getOutboundPortDefinitions() {
        return outboundPortDefinitions;
    }

    public NetworkConfig setOutboundPortDefinitions(final Collection<String> outboundPortDefs) {
        this.outboundPortDefinitions = outboundPortDefs;
        return this;
    }

    public NetworkConfig addOutboundPortDefinition(String portDef) {
        if (outboundPortDefinitions == null) {
            outboundPortDefinitions = new HashSet<String>();
        }
        outboundPortDefinitions.add(portDef);
        return this;
    }

    public Collection<Integer> getOutboundPorts() {
        return outboundPorts;
    }

    public NetworkConfig setOutboundPorts(final Collection<Integer> outboundPorts) {
        this.outboundPorts = outboundPorts;
        return this;
    }

    public NetworkConfig addOutboundPort(int port) {
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
    public NetworkConfig setInterfaces(final InterfacesConfig interfaces) {
        this.interfaces = interfaces;
        return this;
    }

    /**
     * Returns the {@link JoinConfig}.
     *
     * @return the join
     */
    public JoinConfig getJoin() {
        return join;
    }

    /**
     * @param join the join to set
     */
    public NetworkConfig setJoin(final JoinConfig join) {
        this.join = join;
        return this;
    }

    public String getPublicAddress() {
        return publicAddress;
    }

    public NetworkConfig setPublicAddress(String publicAddress) {
        this.publicAddress = publicAddress;
        return this;
    }

    /**
     * Gets the {@link SocketInterceptorConfig}. The value can be null if no socket interception is needed.
     *
     * @return the SocketInterceptorConfig
     * @see #setSocketInterceptorConfig(SocketInterceptorConfig)
     */
    public SocketInterceptorConfig getSocketInterceptorConfig() {
        return socketInterceptorConfig;
    }

    /**
     * Sets the {@link SocketInterceptorConfig}. The value can be null if no socket interception is needed.
     *
     * @param socketInterceptorConfig the SocketInterceptorConfig
     * @return the updated NetworkConfig
     */
    public NetworkConfig setSocketInterceptorConfig(SocketInterceptorConfig socketInterceptorConfig) {
        this.socketInterceptorConfig = socketInterceptorConfig;
        return this;
    }

    /**
     * Gets the {@link SymmetricEncryptionConfig}. The value can be null which means that no symmetric encryption should
     * be used.
     *
     * @return the SymmetricEncryptionConfig
     * @see
     */
    public SymmetricEncryptionConfig getSymmetricEncryptionConfig() {
        return symmetricEncryptionConfig;
    }

    /**
     * Sets the {@link SymmetricEncryptionConfig}. The value can be null if no symmetric encryption should be used.
     *
     * @param symmetricEncryptionConfig the SymmetricEncryptionConfig
     * @return the updated NetworkConfig.
     * @see #getSymmetricEncryptionConfig()
     */
    public NetworkConfig setSymmetricEncryptionConfig(final SymmetricEncryptionConfig symmetricEncryptionConfig) {
        this.symmetricEncryptionConfig = symmetricEncryptionConfig;
        return this;
    }

    /**
     * Returns the current {@link SSLConfig}. It is possible that null is returned if no SSLConfig has been
     * set.
     *
     * @return the SSLConfig.
     * @see #setSSLConfig(SSLConfig)
     */
    public SSLConfig getSSLConfig() {
        return sslConfig;
    }

    /**
     * Sets the {@link SSLConfig}. null value indicates that no SSLConfig should be used.
     *
     * @param sslConfig the SSLConfig.
     * @return the updated NetworkConfig.
     * @see #getSSLConfig()
     */
    public NetworkConfig setSSLConfig(SSLConfig sslConfig) {
        this.sslConfig = sslConfig;
        return this;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("NetworkConfig {");
        sb.append("publicAddress='").append(publicAddress).append('\'');
        sb.append(", port=").append(port);
        sb.append(", portCount=").append(portCount);
        sb.append(", portAutoIncrement=").append(portAutoIncrement);
        sb.append(", join=").append(join);
        sb.append(", interfaces=").append(interfaces);
        sb.append(", sslConfig=").append(sslConfig);
        sb.append(", socketInterceptorConfig=").append(socketInterceptorConfig);
        sb.append(", symmetricEncryptionConfig=").append(symmetricEncryptionConfig);
        sb.append('}');
        return sb.toString();
    }
}
