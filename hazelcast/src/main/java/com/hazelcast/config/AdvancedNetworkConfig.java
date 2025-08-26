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

import com.hazelcast.config.rest.RestConfig;
import com.hazelcast.instance.ProtocolType;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.spi.annotation.Beta;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.instance.EndpointQualifier.CLIENT;
import static com.hazelcast.instance.EndpointQualifier.MEMBER;
import static com.hazelcast.instance.EndpointQualifier.MEMCACHE;
import static com.hazelcast.instance.EndpointQualifier.REST;

/**
 * Similar to {@link NetworkConfig} with the additional ability to define multiple
 * endpoints, each with its own separate protocol/security and/or socket properties.
 * When {@link AdvancedNetworkConfig} is present and enabled then it always takes precedence over
 * the NetworkingConfig.
 *
 * @see #addWanEndpointConfig(EndpointConfig)
 * @see #setClientEndpointConfig(ServerSocketEndpointConfig)
 * @see #setMemberEndpointConfig(ServerSocketEndpointConfig)
 * @see #setMemcacheEndpointConfig(ServerSocketEndpointConfig)
 * @see #setRestEndpointConfig(RestServerEndpointConfig)
 * @since 3.12
 */
@Beta
@SuppressWarnings("checkstyle:methodcount")
public class AdvancedNetworkConfig {

    private boolean enabled;

    private final Map<EndpointQualifier, EndpointConfig> endpointConfigs
            = new ConcurrentHashMap<>();

    private JoinConfig join = new JoinConfig();

    private IcmpFailureDetectorConfig icmpFailureDetectorConfig;

    private MemberAddressProviderConfig memberAddressProviderConfig = new MemberAddressProviderConfig();

    public AdvancedNetworkConfig() {
        endpointConfigs.put(MEMBER, new ServerSocketEndpointConfig().setProtocolType(ProtocolType.MEMBER));
    }

    public MemberAddressProviderConfig getMemberAddressProviderConfig() {
        return memberAddressProviderConfig;
    }

    public AdvancedNetworkConfig setMemberAddressProviderConfig(MemberAddressProviderConfig memberAddressProviderConfig) {
        this.memberAddressProviderConfig = memberAddressProviderConfig;
        return this;
    }

    /**
     * Adds the given WAN {@code EndpointConfig} to the endpoints configuration.  When the argument is an
     * {@link EndpointConfig}, then it configures network settings for the client side of outgoing network
     * connections opened from that member. When the provided {@code endpointconfig} is a
     * {@link ServerSocketEndpointConfig}, then the {@code HazelcastInstance} will additionally listen on
     * the configured port for incoming WAN connections.
     * <p>
     * Example of how to configure Hazelcast instances for active-passive WAN replication:
     * <ul>
     * <li>
     *     On the passive side, configure the Hazelcast instance to listen on port 8765 with SSL enabled:
     * <pre>{@code
     * Config config = new Config();
     * ServerSocketEndpointConfig wanServerSocketConfig = new ServerSocketEndpointConfig().setPort(8765);
     * // setup SSL config etc
     * wanServerSocketConfig.getSSLConfig()
     *                          .setEnabled(true)
     *                          .setFactoryImplementation(new OpenSSLEngineFactory());
     * config.getAdvancedNetworkConfig()
     *           .setEnabled(true)
     *           .addWanEndpointConfig(wanServerSocketConfig);
     *
     * HazelcastInstance passive = Hazelcast.newHazelcastInstance(config);
     * }
     * </pre>
     * </li>
     * <li>
     *     On the active side, configure the client-side of outgoing connections with SSL:
     * <pre>{@code
     * Config config = new Config();
     * EndpointConfig wanEndpointConfig = new EndpointConfig().setName("wan-tokyo");
     * wanEndpointConfig.getSSLConfig()
     *                      .setEnabled(true)
     *                      .setFactoryImplementation(new OpenSSLEngineFactory());
     * config.getAdvancedNetworkConfig()
     *       .setEnabled(true)
     *       .addWanEndpointConfig(wanEndpointConfig);
     * // setup WAN replication
     * WanReplicationConfig wanReplicationConfig = new WanReplicationConfig();
     * // WAN publisher config references endpoint config by name
     * WanBatchReplicationPublisherConfig publisherConfig = new WanBatchReplicationPublisherConfig()
     *                 .setEndpoint("wan-tokyo")
     *                 .setTargetEndpoints("tokyo.hazelcast.com:8765");
     * wanReplicationConfig.addWanBatchReplicationPublisherConfig(publisherConfig);
     * config.addWanReplicationConfig(wanReplicationConfig);
     *
     * HazelcastInstance active = Hazelcast.newHazelcastInstance(config);
     * }
     * </pre>
     * </li>
     * </ul>
     *
     * @param endpointConfig the endpoint configuration to add
     * @return this object for fluent chaining
     */
    public AdvancedNetworkConfig addWanEndpointConfig(EndpointConfig endpointConfig) {
        endpointConfig.setProtocolType(ProtocolType.WAN);
        endpointConfigs.put(endpointConfig.getQualifier(), endpointConfig);
        return this;
    }

    /**
     * Sets the server socket endpoint config for {@link ProtocolType#MEMBER MEMBER} protocol. If another
     * {@code ServerSocketEndpointConfig} was previously set, it will be replaced. When starting a Hazelcast
     * instance with {@code AdvancedNetworkConfig} enabled, its configuration must include
     * one {@code ServerSocketEndpointConfig} for protocol type {@code MEMBER}.
     *
     * @param serverSocketEndpointConfig the server socket endpoint configuration
     * @return this object for fluent chaining
     */
    public AdvancedNetworkConfig setMemberEndpointConfig(ServerSocketEndpointConfig serverSocketEndpointConfig) {
        serverSocketEndpointConfig.setProtocolType(ProtocolType.MEMBER);
        endpointConfigs.put(MEMBER, serverSocketEndpointConfig);
        return this;
    }

    /**
     * Sets the server socket endpoint config for {@link ProtocolType#CLIENT CLIENT} protocol. If another
     * {@code ServerSocketEndpointConfig} was previously set, it will be replaced. When starting a Hazelcast
     * instance with {@code AdvancedNetworkConfig} enabled, its configuration may include
     * at most one {@code ServerSocketEndpointConfig} for protocol type {@code CLIENT}.
     *
     * @param serverSocketEndpointConfig the server socket endpoint configuration
     * @return this object for fluent chaining
     */
    public AdvancedNetworkConfig setClientEndpointConfig(ServerSocketEndpointConfig serverSocketEndpointConfig) {
        serverSocketEndpointConfig.setProtocolType(ProtocolType.CLIENT);
        endpointConfigs.put(CLIENT, serverSocketEndpointConfig);
        return this;
    }

    /**
     * Sets the server socket endpoint config for {@link ProtocolType#REST REST} protocol. If another
     * {@code ServerSocketEndpointConfig} was previously set, it will be replaced. When starting a Hazelcast
     * instance with {@code AdvancedNetworkConfig} enabled, its configuration may include
     * at most one {@code ServerSocketEndpointConfig} for protocol type {@code REST}.
     *
     * @param restServerEndpointConfig the server socket endpoint configuration
     * @return this object for fluent chaining
     *
     * @deprecated use RestConfig instead. Will be removed at 6.0.
     * @see RestConfig
     */
    @Deprecated(since = "5.5", forRemoval = true)
    public AdvancedNetworkConfig setRestEndpointConfig(RestServerEndpointConfig restServerEndpointConfig) {
        restServerEndpointConfig.setProtocolType(ProtocolType.REST);
        endpointConfigs.put(REST, restServerEndpointConfig);
        return this;
    }

    /**
     * Sets the server socket endpoint config for {@link ProtocolType#MEMCACHE memcache} protocol. If another
     * {@code ServerSocketEndpointConfig} was previously set, it will be replaced.
     *
     * @param memcacheEndpointConfig the server socket endpoint configuration
     * @return this object for fluent chaining
     */
    public AdvancedNetworkConfig setMemcacheEndpointConfig(ServerSocketEndpointConfig memcacheEndpointConfig) {
        memcacheEndpointConfig.setProtocolType(ProtocolType.MEMCACHE);
        endpointConfigs.put(MEMCACHE, memcacheEndpointConfig);
        return this;
    }

    public Map<EndpointQualifier, EndpointConfig> getEndpointConfigs() {
        return endpointConfigs;
    }

    public AdvancedNetworkConfig setEndpointConfigs(Map<EndpointQualifier, EndpointConfig> endpointConfigs) {
        // sanitize input
        for (Map.Entry<EndpointQualifier, EndpointConfig> entry : endpointConfigs.entrySet()) {
            entry.getValue().setProtocolType(entry.getKey().getType());
        }
        // validate
        for (ProtocolType protocolType : ProtocolType.values()) {
            int count = countEndpointConfigs(endpointConfigs, protocolType);
            if (count > protocolType.getServerSocketCardinality()) {
                throw new InvalidConfigurationException("Protocol type " + protocolType + " does not allow more than "
                        + protocolType.getServerSocketCardinality() + " server sockets but " + count + " were defined");
            }
        }
        this.endpointConfigs.clear();
        this.endpointConfigs.putAll(endpointConfigs);
        return this;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public AdvancedNetworkConfig setEnabled(boolean enabled) {
        this.enabled = enabled;
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
    public AdvancedNetworkConfig setJoin(final JoinConfig join) {
        this.join = join;
        return this;
    }

    /**
     * Sets the {@link IcmpFailureDetectorConfig}. The value can be {@code null} if this detector isn't needed.
     *
     * @param icmpFailureDetectorConfig the IcmpFailureDetectorConfig to set
     * @return the updated NetworkConfig
     * @see #getIcmpFailureDetectorConfig()
     */
    public AdvancedNetworkConfig setIcmpFailureDetectorConfig(final IcmpFailureDetectorConfig icmpFailureDetectorConfig) {
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

    public RestServerEndpointConfig getRestEndpointConfig() {
        return (RestServerEndpointConfig) endpointConfigs.get(REST);
    }

    @Override
    public String toString() {
        return "AdvancedNetworkConfig{"
                + "isEnabled=" + enabled
                + ", join=" + join
                + ", memberAddressProvider=" + memberAddressProviderConfig
                + ", endpointConfigs=" + endpointConfigs
                + ", icmpFailureDetectorConfig=" + icmpFailureDetectorConfig
                + '}';
    }

    /**
     * Member endpoint decorated as a {@link NetworkConfig}
     * Facade used during bootstrap to hide if-logic between the two networking configuration approaches
     */
    public static class MemberNetworkingView
            extends NetworkConfig {

        private final AdvancedNetworkConfig config;

        MemberNetworkingView(AdvancedNetworkConfig config) {
            this.config = config;
        }

        private ServerSocketEndpointConfig getMemberEndpoint() {
            return (ServerSocketEndpointConfig) config.getEndpointConfigs().get(MEMBER);
        }

        @Override
        public int getPort() {
            return getMemberEndpoint().getPort();
        }

        @Override
        public NetworkConfig setPort(int port) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getPortCount() {
            return getMemberEndpoint().getPortCount();
        }

        @Override
        public NetworkConfig setPortCount(int portCount) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isPortAutoIncrement() {
            return getMemberEndpoint().isPortAutoIncrement();
        }

        @Override
        public NetworkConfig setPortAutoIncrement(boolean portAutoIncrement) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isReuseAddress() {
            return getMemberEndpoint().isReuseAddress();
        }

        @Override
        public NetworkConfig setReuseAddress(boolean reuseAddress) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Collection<String> getOutboundPortDefinitions() {
            return getMemberEndpoint().getOutboundPortDefinitions();
        }

        @Override
        public NetworkConfig setOutboundPortDefinitions(Collection<String> outboundPortDefs) {
            throw new UnsupportedOperationException();
        }

        @Override
        public NetworkConfig addOutboundPortDefinition(String portDef) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Collection<Integer> getOutboundPorts() {
            return getMemberEndpoint().getOutboundPorts();
        }

        @Override
        public NetworkConfig setOutboundPorts(Collection<Integer> outboundPorts) {
            throw new UnsupportedOperationException();
        }

        @Override
        public NetworkConfig addOutboundPort(int port) {
            throw new UnsupportedOperationException();
        }

        @Override
        public InterfacesConfig getInterfaces() {
            return getMemberEndpoint().getInterfaces();
        }

        @Override
        public NetworkConfig setInterfaces(InterfacesConfig interfaces) {
            throw new UnsupportedOperationException();
        }

        @Override
        public JoinConfig getJoin() {
            return config.getJoin();
        }

        @Override
        public NetworkConfig setJoin(JoinConfig join) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getPublicAddress() {
            return getMemberEndpoint().getPublicAddress();
        }

        @Override
        public NetworkConfig setPublicAddress(String publicAddress) {
            throw new UnsupportedOperationException();
        }

        @Override
        public SocketInterceptorConfig getSocketInterceptorConfig() {
            return getMemberEndpoint().getSocketInterceptorConfig();
        }

        @Override
        public NetworkConfig setSocketInterceptorConfig(SocketInterceptorConfig socketInterceptorConfig) {
            throw new UnsupportedOperationException();
        }

        @Override
        public SymmetricEncryptionConfig getSymmetricEncryptionConfig() {
            return getMemberEndpoint().getSymmetricEncryptionConfig();
        }

        @Override
        public NetworkConfig setSymmetricEncryptionConfig(SymmetricEncryptionConfig symmetricEncryptionConfig) {
            throw new UnsupportedOperationException();
        }

        @Override
        public SSLConfig getSSLConfig() {
            return getMemberEndpoint().getSSLConfig();
        }

        @Override
        public NetworkConfig setSSLConfig(SSLConfig sslConfig) {
            throw new UnsupportedOperationException();
        }

        @Override
        public MemberAddressProviderConfig getMemberAddressProviderConfig() {
            return config.getMemberAddressProviderConfig();
        }

        @Override
        public NetworkConfig setMemberAddressProviderConfig(MemberAddressProviderConfig memberAddressProviderConfig) {
            throw new UnsupportedOperationException();
        }

        @Override
        public NetworkConfig setIcmpFailureDetectorConfig(IcmpFailureDetectorConfig icmpFailureDetectorConfig) {
            throw new UnsupportedOperationException();
        }

        @Override
        public IcmpFailureDetectorConfig getIcmpFailureDetectorConfig() {
            return config.getIcmpFailureDetectorConfig();
        }

        @Override
        public String toString() {
            return "NetworkConfig{"
                    + "MemberNetworkingView=true"
                    + ", publicAddress='" + getPublicAddress() + '\''
                    + ", port=" + getPort()
                    + ", portCount=" + getPortCount()
                    + ", portAutoIncrement=" + isPortAutoIncrement()
                    + ", join=" + getJoin()
                    + ", interfaces=" + getInterfaces()
                    + ", sslConfig=" + getSSLConfig()
                    + ", socketInterceptorConfig=" + getSocketInterceptorConfig()
                    + ", symmetricEncryptionConfig=" + getSymmetricEncryptionConfig()
                    + ", icmpFailureDetectorConfig=" + getIcmpFailureDetectorConfig()
                    + ", memcacheProtocolConfig=" + getMemcacheProtocolConfig()
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
            MemberNetworkingView that = (MemberNetworkingView) o;
            return Objects.equals(config, that.config);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), config);
        }
    }

    private static int countEndpointConfigs(Map<EndpointQualifier, EndpointConfig> endpointConfigs,
                                            ProtocolType protocolType) {
        int count = 0;
        for (EndpointQualifier qualifier : endpointConfigs.keySet()) {
            if (qualifier.getType() == protocolType) {
                count++;
            }
        }
        return count;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AdvancedNetworkConfig that = (AdvancedNetworkConfig) o;
        return enabled == that.enabled && Objects.equals(endpointConfigs, that.endpointConfigs)
                && Objects.equals(join, that.join)
                && Objects.equals(icmpFailureDetectorConfig, that.icmpFailureDetectorConfig)
                && Objects.equals(memberAddressProviderConfig, that.memberAddressProviderConfig);
    }

    @Override
    public int hashCode() {
        return Objects.hash(enabled, endpointConfigs, join, icmpFailureDetectorConfig, memberAddressProviderConfig);
    }
}
