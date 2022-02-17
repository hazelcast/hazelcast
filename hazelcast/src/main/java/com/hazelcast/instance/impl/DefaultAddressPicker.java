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

package com.hazelcast.instance.impl;

import com.hazelcast.cluster.Address;
import com.hazelcast.config.Config;
import com.hazelcast.config.EndpointConfig;
import com.hazelcast.config.InterfacesConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.instance.AddressPicker;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.internal.cluster.impl.TcpIpJoiner;
import com.hazelcast.internal.util.AddressUtil;
import com.hazelcast.internal.util.NetworkInterfaceInfo;
import com.hazelcast.internal.util.NetworkInterfacesEnumerator;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.spi.properties.HazelcastProperties;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.channels.ServerSocketChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

import static com.hazelcast.instance.EndpointQualifier.MEMBER;
import static com.hazelcast.instance.impl.ServerSocketHelper.createServerSocketChannel;
import static com.hazelcast.internal.util.AddressUtil.fixScopeIdAndGetInetAddress;
import static com.hazelcast.internal.util.CollectionUtil.isEmpty;
import static com.hazelcast.internal.util.CollectionUtil.isNotEmpty;
import static com.hazelcast.internal.util.MapUtil.createLinkedHashMap;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;

class DefaultAddressPicker
        implements AddressPicker {

    /**
     * See https://docs.oracle.com/javase/8/docs/api/java/net/doc-files/net-properties.html
     */
    static final String PREFER_IPV4_STACK = "java.net.preferIPv4Stack";

    /**
     * See https://docs.oracle.com/javase/8/docs/api/java/net/doc-files/net-properties.html
     */
    static final String PREFER_IPV6_ADDRESSES = "java.net.preferIPv6Addresses";

    private final ILogger logger;

    private final HazelcastProperties hazelcastProperties;

    private final Config config;
    private final InterfacesConfig interfacesConfig;
    private final TcpIpConfig tcpIpConfig;
    private final String publicAddressConfig;
    private final EndpointQualifier endpointQualifier;
    private final boolean isReuseAddress;
    private final boolean isPortAutoIncrement;
    private final int port;
    private final int portCount;

    private HostnameResolver hostnameResolver = new InetAddressHostnameResolver();
    private Address publicAddress;
    private Address bindAddress;
    private ServerSocketChannel serverSocketChannel;
    private NetworkInterfacesEnumerator networkInterfacesEnumerator = NetworkInterfacesEnumerator.defaultEnumerator();

    DefaultAddressPicker(Config config, ILogger logger) {
        this(config, null, config.getNetworkConfig().getInterfaces(), config.getNetworkConfig().getJoin().getTcpIpConfig(),
                config.getNetworkConfig().isReuseAddress(), config.getNetworkConfig().isPortAutoIncrement(),
                config.getNetworkConfig().getPort(), config.getNetworkConfig().getPortCount(),
                config.getNetworkConfig().getPublicAddress(), logger);
    }

    DefaultAddressPicker(Config config, EndpointQualifier endpointQualifier, InterfacesConfig interfacesConfig,
                         TcpIpConfig tcpIpConfig, boolean isReuseAddress, boolean isPortAutoIncrement, int port, int portCount,
                         String publicAddressConfig, ILogger logger) {
        this.logger = logger;
        this.isReuseAddress = isReuseAddress;
        this.isPortAutoIncrement = isPortAutoIncrement;
        this.port = port;
        this.portCount = portCount;
        this.endpointQualifier = endpointQualifier;
        this.interfacesConfig = interfacesConfig;
        this.tcpIpConfig = tcpIpConfig;
        this.publicAddressConfig = publicAddressConfig;
        this.hazelcastProperties = new HazelcastProperties(config);
        this.config = config;
    }

    @Override
    public void pickAddress() throws Exception {
        if (publicAddress != null || bindAddress != null) {
            return;
        }
        try {
            AddressDefinition publicAddressDef = getPublicAddressByPortSearch();
            if (publicAddressDef != null) {
                publicAddress = createAddress(publicAddressDef, publicAddressDef.port);
                logger.info("Using public address: " + publicAddress);
            } else {
                publicAddress = bindAddress;
                logger.finest("Using public address the same as the bind address: " + publicAddress);
            }
        } catch (Exception e) {
            ServerSocketChannel serverSocketChannel = getServerSocketChannel(endpointQualifier);
            if (serverSocketChannel != null) {
                serverSocketChannel.close();
            }
            logger.severe(e);
            throw e;
        }
    }

    private AddressDefinition getPublicAddressByPortSearch() throws IOException {
        boolean bindAny = hazelcastProperties.getBoolean(ClusterProperty.SOCKET_SERVER_BIND_ANY);
        AddressDefinition bindAddressDef = pickAddressDef();

        EndpointConfig endpoint = config.getAdvancedNetworkConfig().isEnabled()
                ? config.getAdvancedNetworkConfig().getEndpointConfigs().get(endpointQualifier)
                : null;

        serverSocketChannel = createServerSocketChannel(logger, endpoint, bindAddressDef.inetAddress,
                bindAddressDef.port == 0 ? port : bindAddressDef.port, portCount, isPortAutoIncrement, isReuseAddress, bindAny);

        int port = serverSocketChannel.socket().getLocalPort();
        bindAddress = createAddress(bindAddressDef, port);

        if (logger.isFineEnabled()) {
            logger.fine("Picked " + bindAddress + (endpointQualifier == null ? "" : ", for endpoint " + endpointQualifier)
                    + ", using socket " + serverSocketChannel.socket() + ", bind any local is " + bindAny);
        }

        return getPublicAddress(port);
    }

    private static Address createAddress(AddressDefinition addressDef, int port) {
        if (addressDef.host == null) {
            return new Address(addressDef.inetAddress, port);
        }
        return new Address(addressDef.host, addressDef.inetAddress, port);
    }

    private AddressDefinition pickAddressDef() throws UnknownHostException, SocketException {
        AddressDefinition addressDef = getSystemConfiguredAddress();
        if (addressDef == null) {
            addressDef = pickInterfaceAddressDef();
        }
        if (addressDef != null) {
            // check if scope ID is set correctly
            addressDef.inetAddress = fixScopeIdAndGetInetAddress(addressDef.inetAddress);
        }
        if (addressDef == null) {
            addressDef = pickLoopbackAddress(null);
        }
        return addressDef;
    }

    private AddressDefinition pickInterfaceAddressDef() throws UnknownHostException, SocketException {
        Collection<InterfaceDefinition> interfaces = getInterfaces();
        if (interfaces.contains(new InterfaceDefinition("localhost", "127.0.0.1"))) {
            return pickLoopbackAddress("localhost");
        }
        if (interfaces.contains(new InterfaceDefinition("127.0.0.1"))) {
            return pickLoopbackAddress(null);
        }

        if (logger.isFineEnabled()) {
            logger.fine("Prefer IPv4 stack is " + preferIPv4Stack() + ", prefer IPv6 addresses is " + preferIPv6Addresses());
        }

        if (!interfaces.isEmpty()) {
            AddressDefinition addressDef = pickMatchingAddress(interfaces);
            if (addressDef != null) {
                return addressDef;
            }
        }

        if (interfacesConfig.isEnabled()) {
            String msg = "Hazelcast CANNOT start on this node. No matching network interface found.\n"
                    + "Interface matching must be either disabled or updated in the hazelcast.xml config file.";
            logger.severe(msg);
            throw new RuntimeException(msg);
        }
        if (tcpIpConfig.isEnabled()) {
            logger.warning("Could not find a matching address to start with! Picking one of non-loopback addresses.");
        }
        return pickMatchingAddress(null);
    }

    private List<InterfaceDefinition> getInterfaces() {
        // address -> domain
        Map<String, String> addressDomainMap = createAddressToDomainMap(tcpIpConfig);

        // must preserve insertion order
        List<InterfaceDefinition> interfaceDefs = new ArrayList<>();
        if (interfacesConfig.isEnabled()) {
            Collection<String> configInterfaces = interfacesConfig.getInterfaces();
            for (String configInterface : configInterfaces) {
                if (!AddressUtil.isIpAddress(configInterface)) {
                    logger.warning("'" + configInterface + "' is not an IP address! Removing from interface list.");
                    continue;
                }
                // add interfaces matching to members in TcpIpConfig
                appendMatchingInterfaces(interfaceDefs, addressDomainMap, configInterface);
                // add default interface definition
                interfaceDefs.add(new InterfaceDefinition(null, configInterface));
            }
            logger.info("Interfaces is enabled, trying to pick one address matching to one of: " + interfaceDefs);
        } else if (tcpIpConfig.isEnabled()) {
            for (Entry<String, String> entry : addressDomainMap.entrySet()) {
                interfaceDefs.add(new InterfaceDefinition(entry.getValue(), entry.getKey()));
            }
            logger.info("Interfaces is disabled, trying to pick one address from TCP-IP config addresses: " + interfaceDefs);
        }
        return interfaceDefs;
    }

    private Map<String, String> createAddressToDomainMap(TcpIpConfig tcpIpConfig) {
        if (!tcpIpConfig.isEnabled()) {
            return Collections.emptyMap();
        }

        Collection<String> possibleAddresses = TcpIpJoiner.getConfigurationMembers(tcpIpConfig);
        // LinkedHashMap is to guarantee order
        Map<String, String> addressDomainMap = createLinkedHashMap(possibleAddresses.size());
        for (String possibleAddress : possibleAddresses) {
            String addressHolder = AddressUtil.getAddressHolder(possibleAddress).getAddress();
            if (AddressUtil.isIpAddress(addressHolder)) {
                // there may be a domain registered for this address
                if (!addressDomainMap.containsKey(addressHolder)) {
                    addressDomainMap.put(addressHolder, null);
                }
            } else {
                try {
                    Collection<String> addresses = resolveDomainNames(addressHolder);
                    for (String address : addresses) {
                        addressDomainMap.put(address, addressHolder);
                    }
                } catch (UnknownHostException e) {
                    logger.warning("Cannot resolve hostname: '" + addressHolder + "'");
                }
            }
        }
        return addressDomainMap;
    }

    private static void appendMatchingInterfaces(Collection<InterfaceDefinition> interfaces,
                                                 Map<String, String> address2DomainMap, String configInterface) {

        for (Entry<String, String> entry : address2DomainMap.entrySet()) {
            String address = entry.getKey();
            if (AddressUtil.matchInterface(address, configInterface)) {
                interfaces.add(new InterfaceDefinition(entry.getValue(), address));
            }
        }
    }

    private Collection<String> resolveDomainNames(String domainName) throws UnknownHostException {
        Collection<String> addresses = hostnameResolver.resolve(domainName);
        logger.warning("You configured your member address as host name. "
                + "Please be aware of that your dns can be spoofed. "
                + "Make sure that your dns configurations are correct.");
        logger.info("Resolving domain name '" + domainName + "' to address(es): " + addresses);
        return addresses;
    }

    private AddressDefinition getSystemConfiguredAddress() throws UnknownHostException {
        String address = config.getProperty("hazelcast.local.localAddress");
        if (address != null) {
            address = address.trim();
            if ("127.0.0.1".equals(address) || "localhost".equals(address)) {
                return pickLoopbackAddress(address);
            } else {
                logger.info("Picking address configured by property 'hazelcast.local.localAddress'");
                return new AddressDefinition(address, InetAddress.getByName(address));
            }
        }
        return null;
    }

    private AddressDefinition getPublicAddress(int port) throws UnknownHostException {
        String address = config.getProperty("hazelcast.local.publicAddress");
        if (address == null) {
            address = publicAddressConfig;
        }
        if (address != null) {
            address = address.trim();
            if ("127.0.0.1".equals(address) || "localhost".equals(address)) {
                return pickLoopbackAddress(address, port);
            } else {
                // allow port to be defined in same string in the form of <host>:<port>, e.g. 10.0.0.0:1234
                AddressUtil.AddressHolder holder = AddressUtil.getAddressHolder(address, port);
                return new AddressDefinition(holder.getAddress(), holder.getPort(), InetAddress.getByName(holder.getAddress()));
            }
        }
        return null;
    }

    private static AddressDefinition pickLoopbackAddress(String host) throws UnknownHostException {
        return new AddressDefinition(host, InetAddress.getByName("127.0.0.1"));
    }

    private static AddressDefinition pickLoopbackAddress(String host, int defaultPort) throws UnknownHostException {
        InetAddress address = InetAddress.getByName("127.0.0.1");
        return new AddressDefinition(host, defaultPort, address);
    }

    AddressDefinition pickMatchingAddress(Collection<InterfaceDefinition> interfaces) throws SocketException {
        List<NetworkInterfaceInfo> networkInterfaces = networkInterfacesEnumerator.getNetworkInterfaces();
        boolean preferIPv4Stack = preferIPv4Stack();
        boolean preferIPv6Addresses = preferIPv6Addresses();
        AddressDefinition matchingAddress = null;

        // There are 3 possible value pairs for preferIPv4Stack & preferIPv6Addresses:
        // - preferIPv4Stack=true, preferIPv6Addresses=false: Only an IPv4 address will be picked.
        // - preferIPv4Stack=false, preferIPv6Addresses=false: Either an IPv4 or IPv6 address may be picked, no preference.
        // - preferIPv4Stack=false, preferIPv6Addresses=true: Either an IPv4 or IPv6 address may be picked
        // but IPv6 address will be preferred over IPv4.

        for (NetworkInterfaceInfo ni : networkInterfaces) {
            if (isEmpty(interfaces) && skipInterface(ni)) {
                continue;
            }
            for (InetAddress inetAddress : ni.getInetAddresses()) {
                if (preferIPv4Stack && inetAddress instanceof Inet6Address) {
                    // IPv4 stack is preferred, so only IPv4 address can be picked.
                    continue;
                }

                AddressDefinition address = getMatchingAddress(interfaces, inetAddress);
                if (address == null) {
                    continue;
                }
                matchingAddress = address;

                if (preferIPv6Addresses) {
                    // IPv6 address is preferred, return if address is IPv6.
                    if (inetAddress instanceof Inet6Address) {
                        return matchingAddress;
                    }
                } else if (inetAddress instanceof Inet4Address) {
                    // No IPv6 address preference, return if address is IPv4.
                    return matchingAddress;
                }
            }
        }
        // nothing matched to IP version preference, return what we have.
        return matchingAddress;
    }

    private AddressDefinition getMatchingAddress(Collection<InterfaceDefinition> interfaces, InetAddress inetAddress) {
        if (isNotEmpty(interfaces)) {
            return match(inetAddress, interfaces);
        } else if (!inetAddress.isLoopbackAddress()) {
            return new AddressDefinition(inetAddress);
        }
        return null;
    }

    /**
     * Checks given network interface and returns true when it should not be used for picking address. Reasons for skipping are
     * the interface is: down, virtual or loopback.
     */
    private boolean skipInterface(NetworkInterfaceInfo ni) throws SocketException {
        boolean skipInterface = !ni.isUp() || ni.isVirtual() || ni.isLoopback();
        if (skipInterface && logger.isFineEnabled()) {
            logger.fine("Skipping NetworkInterface '" + ni.getName() + "': isUp=" + ni.isUp() + ", isVirtual=" + ni.isVirtual()
                    + ", isLoopback=" + ni.isLoopback());
        }
        return skipInterface;
    }

    private AddressDefinition match(InetAddress address, Collection<InterfaceDefinition> interfaces) {
        for (InterfaceDefinition inf : interfaces) {
            if (AddressUtil.matchInterface(address.getHostAddress(), inf.address)) {
                return new AddressDefinition(inf.host, address);
            }
        }
        return null;
    }

    private boolean preferIPv4Stack() {
        return Boolean.getBoolean(PREFER_IPV4_STACK)
                || hazelcastProperties.getBoolean(ClusterProperty.PREFER_IPv4_STACK);
    }

    private boolean preferIPv6Addresses() {
        return !preferIPv4Stack() && Boolean.getBoolean(PREFER_IPV6_ADDRESSES);
    }

    @Override
    public Address getBindAddress(EndpointQualifier qualifier) {
        return bindAddress;
    }

    @Override
    public Address getPublicAddress(EndpointQualifier qualifier) {
        return publicAddress;
    }

    @Override
    public ServerSocketChannel getServerSocketChannel(EndpointQualifier qualifier) {
        return serverSocketChannel;
    }

    @Override
    public Map<EndpointQualifier, ServerSocketChannel> getServerSocketChannels() {
        return Collections.singletonMap(MEMBER, serverSocketChannel);
    }

    @Override
    public Map<EndpointQualifier, Address> getPublicAddressMap() {
        HashMap<EndpointQualifier, Address> publicAddressMap = new HashMap<>();
        publicAddressMap.put(MEMBER, publicAddress);
        return publicAddressMap;
    }

    @Override
    public Map<EndpointQualifier, Address> getBindAddressMap() {
        HashMap<EndpointQualifier, Address> bindAddressMap = new HashMap<>();
        bindAddressMap.put(MEMBER, bindAddress);
        return bindAddressMap;
    }

    void setHostnameResolver(HostnameResolver hostnameResolver) {
        this.hostnameResolver = checkNotNull(hostnameResolver);
    }

    void setNetworkInterfacesEnumerator(NetworkInterfacesEnumerator networkInterfacesEnumerator) {
        this.networkInterfacesEnumerator = networkInterfacesEnumerator;
    }

    static class InterfaceDefinition {

        String host;
        String address;

        InterfaceDefinition(String address) {
            this.host = null;
            this.address = address;
        }

        InterfaceDefinition(String host, String address) {
            this.host = host;
            this.address = address;
        }

        @Override
        public String toString() {
            return host != null ? (host + "/" + address) : address;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            InterfaceDefinition that = (InterfaceDefinition) o;
            if (!Objects.equals(address, that.address)) {
                return false;
            }
            return Objects.equals(host, that.host);
        }

        @Override
        public int hashCode() {
            int result = host != null ? host.hashCode() : 0;
            result = 31 * result + (address != null ? address.hashCode() : 0);
            return result;
        }
    }

    static class AddressDefinition extends InterfaceDefinition {

        InetAddress inetAddress;
        int port;

        AddressDefinition(InetAddress inetAddress) {
            super(inetAddress.getHostAddress());
            this.inetAddress = inetAddress;
        }

        AddressDefinition(String host, InetAddress inetAddress) {
            super(host, inetAddress.getHostAddress());
            this.inetAddress = inetAddress;
        }

        AddressDefinition(String host, int port, InetAddress inetAddress) {
            super(host, inetAddress.getHostAddress());
            this.inetAddress = inetAddress;
            this.port = port;
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

            AddressDefinition that = (AddressDefinition) o;
            if (port != that.port) {
                return false;
            }
            return Objects.equals(inetAddress, that.inetAddress);
        }

        @Override
        public int hashCode() {
            int result = super.hashCode();
            result = 31 * result + (inetAddress != null ? inetAddress.hashCode() : 0);
            result = 31 * result + port;
            return result;
        }
    }


    @FunctionalInterface
    interface HostnameResolver {
        Collection<String> resolve(String hostname) throws UnknownHostException;
    }

    private static class InetAddressHostnameResolver implements HostnameResolver {
        @Override
        public Collection<String> resolve(String hostname) throws UnknownHostException {
            InetAddress[] inetAddresses = InetAddress.getAllByName(hostname);
            Collection<String> addresses = new LinkedList<>();
            for (InetAddress inetAddress : inetAddresses) {
                addresses.add(inetAddress.getHostAddress());
            }
            return addresses;
        }
    }
}
