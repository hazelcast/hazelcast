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

import com.hazelcast.security.jsm.HazelcastRuntimePermission;
import com.hazelcast.internal.util.StringUtil;

import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;

/**
 * Contains configuration for Network.
 */
@SuppressWarnings("checkstyle:methodcount")
public class NetworkConfig {

    /**
     * Default value of port number.
     */
    public static final int DEFAULT_PORT = 5701;

    private static final int PORT_MAX = 0xFFFF;

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

    private MemberAddressProviderConfig memberAddressProviderConfig = new MemberAddressProviderConfig();

    private IcmpFailureDetectorConfig icmpFailureDetectorConfig;

    private RestApiConfig restApiConfig = new RestApiConfig();

    private MemcacheProtocolConfig memcacheProtocolConfig = new MemcacheProtocolConfig();

    public NetworkConfig() {
        String os = StringUtil.lowerCaseInternal(System.getProperty("os.name"));
        reuseAddress = (!os.contains("win"));
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
     * <p>
     * A valid port value is between 0 and 65535.
     * A port number of 0 will let the system pick up an ephemeral port.
     *
     * @param port the port the Hazelcast member will try to bind on
     * @return NetworkConfig the updated NetworkConfig
     * @see #getPort()
     * @see #setPortAutoIncrement(boolean) for more information
     */
    public NetworkConfig setPort(int port) {
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
     * @return this configuration
     * @see #setPortAutoIncrement(boolean) for more information
     */
    public NetworkConfig setPortCount(int portCount) {
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
    public NetworkConfig setPortAutoIncrement(boolean portAutoIncrement) {
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

    /**
     * Overrides the public address of a member.
     * Behind a NAT, two endpoints may not be able to see/access each other.
     * If both nodes set their public addresses to their defined addresses on NAT, then that way
     * they can communicate with each other.
     * It should be set in the format “host IP address:port number”.
     */
    public NetworkConfig setPublicAddress(String publicAddress) {
        this.publicAddress = publicAddress;
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
    public NetworkConfig setSocketInterceptorConfig(SocketInterceptorConfig socketInterceptorConfig) {
        this.socketInterceptorConfig = socketInterceptorConfig;
        return this;
    }

    /**
     * Gets the {@link SymmetricEncryptionConfig}. The value can be {@code null} which means that no symmetric encryption should
     * be used.
     *
     * @return the SymmetricEncryptionConfig
     * @deprecated since 4.2
     */
    @Deprecated
    public SymmetricEncryptionConfig getSymmetricEncryptionConfig() {
        return symmetricEncryptionConfig;
    }

    /**
     * Sets the {@link SymmetricEncryptionConfig}. The value can be {@code null} if no symmetric encryption should be used.
     *
     * @param symmetricEncryptionConfig the SymmetricEncryptionConfig to set
     * @return the updated NetworkConfig
     * @see #getSymmetricEncryptionConfig()
     * @deprecated since 4.2
     */
    @Deprecated
    public NetworkConfig setSymmetricEncryptionConfig(final SymmetricEncryptionConfig symmetricEncryptionConfig) {
        this.symmetricEncryptionConfig = symmetricEncryptionConfig;
        return this;
    }

    /**
     * Returns the current {@link SSLConfig}. It is possible that null is returned if no SSLConfig has been set.
     *
     * @return the SSLConfig
     * @throws SecurityException If a security manager exists and the calling method doesn't have corresponding
     *                           {@link HazelcastRuntimePermission}
     * @see #setSSLConfig(SSLConfig)
     */
    public SSLConfig getSSLConfig() {
        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            sm.checkPermission(new HazelcastRuntimePermission("com.hazelcast.config.NetworkConfig.getSSLConfig"));
        }
        return sslConfig;
    }

    /**
     * Sets the {@link SSLConfig}. null value indicates that no SSLConfig should be used.
     *
     * @param sslConfig the SSLConfig
     * @return the updated NetworkConfig
     * @throws SecurityException If a security manager exists and the calling method doesn't have corresponding
     *                           {@link HazelcastRuntimePermission}
     * @see #getSSLConfig()
     */
    public NetworkConfig setSSLConfig(SSLConfig sslConfig) {
        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            sm.checkPermission(new HazelcastRuntimePermission("com.hazelcast.config.NetworkConfig.setSSLConfig"));
        }
        this.sslConfig = sslConfig;
        return this;
    }

    public MemberAddressProviderConfig getMemberAddressProviderConfig() {
        return memberAddressProviderConfig;
    }

    public NetworkConfig setMemberAddressProviderConfig(MemberAddressProviderConfig memberAddressProviderConfig) {
        this.memberAddressProviderConfig = memberAddressProviderConfig;
        return this;
    }

    /**
     * Sets the {@link IcmpFailureDetectorConfig}. The value can be {@code null} if this detector isn't needed.
     *
     * @param icmpFailureDetectorConfig the IcmpFailureDetectorConfig to set
     * @return the updated NetworkConfig
     * @see #getIcmpFailureDetectorConfig()
     */
    public NetworkConfig setIcmpFailureDetectorConfig(final IcmpFailureDetectorConfig icmpFailureDetectorConfig) {
        this.icmpFailureDetectorConfig = icmpFailureDetectorConfig;
        return this;
    }

    /**
     * Returns the current {@link IcmpFailureDetectorConfig}. It is possible that null is returned if no
     * IcmpFailureDetectorConfig has been set.
     *
     * @return the IcmpFailureDetectorConfig
     * @see #setIcmpFailureDetectorConfig(IcmpFailureDetectorConfig)
     */
    public IcmpFailureDetectorConfig getIcmpFailureDetectorConfig() {
        return icmpFailureDetectorConfig;
    }

    public RestApiConfig getRestApiConfig() {
        return restApiConfig;
    }

    public NetworkConfig setRestApiConfig(RestApiConfig restApiConfig) {
        this.restApiConfig = restApiConfig;
        return this;
    }

    public MemcacheProtocolConfig getMemcacheProtocolConfig() {
        return memcacheProtocolConfig;
    }

    public NetworkConfig setMemcacheProtocolConfig(MemcacheProtocolConfig memcacheProtocolConfig) {
        this.memcacheProtocolConfig = memcacheProtocolConfig;
        return this;
    }

    @Override
    public String toString() {
        return "NetworkConfig{"
                + "publicAddress='" + publicAddress + '\''
                + ", port=" + port
                + ", portCount=" + portCount
                + ", portAutoIncrement=" + portAutoIncrement
                + ", join=" + join
                + ", interfaces=" + interfaces
                + ", sslConfig=" + sslConfig
                + ", socketInterceptorConfig=" + socketInterceptorConfig
                + ", symmetricEncryptionConfig=" + symmetricEncryptionConfig
                + ", icmpFailureDetectorConfig=" + icmpFailureDetectorConfig
                + ", restApiConfig=" + restApiConfig
                + ", memcacheProtocolConfig=" + memcacheProtocolConfig
                + '}';
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
        NetworkConfig that = (NetworkConfig) o;
        return port == that.port && portCount == that.portCount && portAutoIncrement == that.portAutoIncrement
                && reuseAddress == that.reuseAddress && Objects.equals(publicAddress, that.publicAddress)
                && Objects.equals(outboundPortDefinitions, that.outboundPortDefinitions)
                && Objects.equals(outboundPorts, that.outboundPorts)
                && Objects.equals(interfaces, that.interfaces) && Objects.equals(join, that.join)
                && Objects.equals(symmetricEncryptionConfig, that.symmetricEncryptionConfig)
                && Objects.equals(socketInterceptorConfig, that.socketInterceptorConfig)
                && Objects.equals(sslConfig, that.sslConfig)
                && Objects.equals(memberAddressProviderConfig, that.memberAddressProviderConfig)
                && Objects.equals(icmpFailureDetectorConfig, that.icmpFailureDetectorConfig)
                && Objects.equals(restApiConfig, that.restApiConfig)
                && Objects.equals(memcacheProtocolConfig, that.memcacheProtocolConfig);
    }

    @Override
    public int hashCode() {
        return Objects
                .hash(port, portCount, portAutoIncrement, reuseAddress, publicAddress, outboundPortDefinitions, outboundPorts,
                        interfaces, join, symmetricEncryptionConfig, socketInterceptorConfig, sslConfig,
                        memberAddressProviderConfig,
                        icmpFailureDetectorConfig, restApiConfig, memcacheProtocolConfig);
    }
}
