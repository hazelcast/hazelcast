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

package com.hazelcast.instance;

import com.hazelcast.cluster.TcpIpJoiner;
import com.hazelcast.config.AwsConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.util.AddressUtil;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.channels.ServerSocketChannel;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Level;

class DefaultAddressPicker implements AddressPicker {

    private final ILogger logger;
    private final Node node;
    private ServerSocketChannel serverSocketChannel;
    private Address publicAddress;
    private Address bindAddress;

    public DefaultAddressPicker(Node node) {
        this.node = node;
        this.logger = node.getLogger(DefaultAddressPicker.class);
    }

    @Override
    public void pickAddress() throws Exception {
        if (publicAddress != null || bindAddress != null) {
            return;
        }
        try {
            final NetworkConfig networkConfig = node.getConfig().getNetworkConfig();
            final AddressDefinition bindAddressDef = pickAddress(networkConfig);
            final boolean reuseAddress = networkConfig.isReuseAddress();
            final boolean bindAny = node.getGroupProperties().SOCKET_SERVER_BIND_ANY.getBoolean();
            final int portCount = networkConfig.getPortCount();
            /**
             * why setReuseAddress(true)?
             * when the member is shutdown,
             * the server socket port will be in TIME_WAIT state for the next
             * 2 minutes or so. If you start the member right after shutting it down
             * you may not be able to bind to the same port because it is in TIME_WAIT
             * state. if you set reuseAddress=true then TIME_WAIT will be ignored and
             * you will be able to bind to the same port again.
             *
             * this will cause problem on windows
             * see http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6421091
             * http://www.hsc.fr/ressources/articles/win_net_srv/multiple_bindings.html
             *
             * By default if the OS is Windows then reuseAddress will be false.
             */
            log(Level.FINEST, "inet reuseAddress:" + reuseAddress);
            InetSocketAddress inetSocketAddress;
            ServerSocket serverSocket = null;
            int port = networkConfig.getPort();

            Throwable error = null;
            for (int i = 0; i < portCount; i++) {
                /**
                 * Instead of reusing the ServerSocket/ServerSocketChannel, we are going to close and replace them on
                 * every attempt to find a free port. The reason to do this is because in some cases, when concurrent
                 * threads/processes try to acquire the same port, the ServerSocket gets corrupted and isn't able to
                 * find any free port at all (no matter if there are more than enough free ports available). We have
                 * seen this happening on Linux and Windows environments.
                 */
                serverSocketChannel = ServerSocketChannel.open();
                serverSocket = serverSocketChannel.socket();
                serverSocket.setReuseAddress(reuseAddress);
                serverSocket.setSoTimeout(1000);
                try {
                    if (bindAny) {
                        inetSocketAddress = new InetSocketAddress(port);
                    } else {
                        inetSocketAddress = new InetSocketAddress(bindAddressDef.inetAddress, port);
                    }
                    log(Level.FINEST, "Trying to bind inet socket address:" + inetSocketAddress);
                    serverSocket.bind(inetSocketAddress, 100);
                    log(Level.FINEST, "Bind successful to inet socket address:" + inetSocketAddress);
                    break;
                } catch (final Exception e) {
                    serverSocket.close();
                    serverSocketChannel.close();

                    if (networkConfig.isPortAutoIncrement()) {
                        port++;
                        error = e;
                    } else {
                        String msg = "Port [" + port + "] is already in use and auto-increment is "
                                + "disabled. Hazelcast cannot start.";
                        logger.severe(msg, e);
                        throw new HazelcastException(msg, error);
                    }
                }
            }
            if (serverSocket == null || !serverSocket.isBound()) {
                throw new HazelcastException("ServerSocket bind has failed. Hazelcast cannot start! "
                        + "config-port: " + networkConfig.getPort() + ", latest-port: " + port, error);
            }
            serverSocketChannel.configureBlocking(false);
            bindAddress = createAddress(bindAddressDef, port);
            log(Level.INFO, "Picked " + bindAddress + ", using socket " + serverSocket + ", bind any local is "
                    + bindAny);
            AddressDefinition publicAddressDef = getPublicAddress(node.getConfig(), port);
            if (publicAddressDef != null) {
                publicAddress = createAddress(publicAddressDef, publicAddressDef.port);
                log(Level.INFO, "Using public address: " + publicAddress);
            } else {
                publicAddress = bindAddress;
                log(Level.FINEST, "Using public address the same as the bind address. " + publicAddress);
            }
        } catch (RuntimeException re) {
            logger.severe(re);
            throw re;
        } catch (Exception e) {
            logger.severe(e);
            throw e;
        }
    }

    private Address createAddress(final AddressDefinition addressDef, final int port) throws UnknownHostException {
        return addressDef.host != null ? new Address(addressDef.host, port)
                : new Address(addressDef.inetAddress, port);
    }

    private AddressDefinition pickAddress(final NetworkConfig networkConfig)
            throws UnknownHostException, SocketException {
        AddressDefinition addressDef = getSystemConfiguredAddress(node.getConfig());
        if (addressDef == null) {
            final Collection<InterfaceDefinition> interfaces = getInterfaces(networkConfig);
            if (interfaces.contains(new InterfaceDefinition("127.0.0.1"))
                    || interfaces.contains(new InterfaceDefinition("localhost"))) {
                addressDef = pickLoopbackAddress();
            } else {
                if (preferIPv4Stack()) {
                    log(Level.INFO, "Prefer IPv4 stack is true.");
                }
                if (interfaces.size() > 0) {
                    addressDef = pickMatchingAddress(interfaces);
                }
                if (addressDef == null) {
                    if (networkConfig.getInterfaces().isEnabled()) {
                        String msg = "Hazelcast CANNOT start on this node. No matching network interface found. ";
                        msg += "\nInterface matching must be either disabled or updated in the hazelcast.xml config file.";
                        logger.severe(msg);
                        throw new RuntimeException(msg);
                    } else {
                        if (networkConfig.getJoin().getTcpIpConfig().isEnabled()) {
                            logger.warning("Could not find a matching address to start with! "
                                    + "Picking one of non-loopback addresses.");
                        }
                        addressDef = pickMatchingAddress(null);
                    }
                }
            }
        }
        if (addressDef != null) {
            // check if scope id correctly set
            addressDef.inetAddress = AddressUtil.fixScopeIdAndGetInetAddress(addressDef.inetAddress);
        }
        if (addressDef == null) {
            addressDef = pickLoopbackAddress();
        }
        return addressDef;
    }

    private Collection<InterfaceDefinition> getInterfaces(final NetworkConfig networkConfig)
            throws UnknownHostException {
        final Map<String, String> addressDomainMap; // address -> domain
        final TcpIpConfig tcpIpConfig = networkConfig.getJoin().getTcpIpConfig();
        if (tcpIpConfig.isEnabled()) {
            addressDomainMap = new LinkedHashMap<String, String>();  // LinkedHashMap is to guarantee order
            final Collection<String> possibleAddresses = TcpIpJoiner.getConfigurationMembers(node.config);
            for (String possibleAddress : possibleAddresses) {
                final String s = AddressUtil.getAddressHolder(possibleAddress).getAddress();
                if (AddressUtil.isIpAddress(s)) {
                    if (!addressDomainMap.containsKey(s)) { // there may be a domain registered for this address
                        addressDomainMap.put(s, null);
                    }
                } else {
                    try {
                        final Collection<String> addresses = resolveDomainNames(s);
                        for (String address : addresses) {
                            addressDomainMap.put(address, s);
                        }
                    } catch (UnknownHostException e) {
                        logger.severe("Could not resolve address: " + s);
                    }
                }
            }
        } else {
            addressDomainMap = Collections.emptyMap();
        }
        final Collection<InterfaceDefinition> interfaces = new HashSet<InterfaceDefinition>();
        if (networkConfig.getInterfaces().isEnabled()) {
            final Collection<String> configInterfaces = networkConfig.getInterfaces().getInterfaces();
            for (String configInterface : configInterfaces) {
                if (AddressUtil.isIpAddress(configInterface)) {
                    String hostname = findHostnameMatchingInterface(addressDomainMap, configInterface);
                    interfaces.add(new InterfaceDefinition(hostname, configInterface));
                } else {
                    logger.info("'" + configInterface
                            + "' is not an IP address! Removing from interface list.");
                }
            }
            log(Level.INFO, "Interfaces is enabled, trying to pick one address matching "
                    + "to one of: " + interfaces);
        } else if (tcpIpConfig.isEnabled()) {
            for (Entry<String, String> entry : addressDomainMap.entrySet()) {
                interfaces.add(new InterfaceDefinition(entry.getValue(), entry.getKey()));
            }
            log(Level.INFO, "Interfaces is disabled, trying to pick one address from TCP-IP config "
                    + "addresses: " + interfaces);
        }
        return interfaces;
    }

    private String findHostnameMatchingInterface(Map<String, String> addressDomainMap, String configInterface) {
        String hostname = addressDomainMap.get(configInterface);
        if (hostname != null) {
            return hostname;
        }
        for (Entry<String, String> entry : addressDomainMap.entrySet()) {
            String address = entry.getKey();
            if (AddressUtil.matchInterface(address, configInterface)) {
                return entry.getValue();
            }
        }
        return null;
    }

    private Collection<String> resolveDomainNames(final String domainName)
            throws UnknownHostException {
        final InetAddress[] inetAddresses = InetAddress.getAllByName(domainName);
        final Collection<String> addresses = new LinkedList<String>();
        for (InetAddress inetAddress : inetAddresses) {
            addresses.add(inetAddress.getHostAddress());
        }
        logger.info("Resolving domain name '" + domainName + "' to address(es): " + addresses);
        return addresses;
    }

    private AddressDefinition getSystemConfiguredAddress(Config config) throws UnknownHostException {
        String address = config.getProperty("hazelcast.local.localAddress");
        if (address != null) {
            address = address.trim();
            if ("127.0.0.1".equals(address) || "localhost".equals(address)) {
                return pickLoopbackAddress();
            } else {
                log(Level.INFO, "Picking address configured by property 'hazelcast.local.localAddress'");
                return new AddressDefinition(address, InetAddress.getByName(address));
            }
        }
        return null;
    }

    private AddressDefinition getPublicAddress(Config config, int defaultPort) throws UnknownHostException {
        String address = config.getProperty("hazelcast.local.publicAddress");
        if (address == null) {
            address = config.getNetworkConfig().getPublicAddress();
        }
        if (address != null) {
            address = address.trim();
            if ("127.0.0.1".equals(address) || "localhost".equals(address)) {
                return pickLoopbackAddress();
            } else {
                // Allow port to be defined in same string in the form of <host>:<port>. i.e. 10.0.0.0:1234
                AddressUtil.AddressHolder holder = AddressUtil.getAddressHolder(address, defaultPort);
                return new AddressDefinition(holder.getAddress(), holder.getPort(), InetAddress.getByName(holder.getAddress()));
            }
        }
        return null;
    }

    private AddressDefinition pickLoopbackAddress() throws UnknownHostException {
        return new AddressDefinition(InetAddress.getByName("127.0.0.1"));
    }

    private AddressDefinition pickMatchingAddress(final Collection<InterfaceDefinition> interfaces) throws SocketException {
        final Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
        final boolean preferIPv4Stack = preferIPv4Stack();
        while (networkInterfaces.hasMoreElements()) {
            final NetworkInterface ni = networkInterfaces.nextElement();
            final Enumeration<InetAddress> e = ni.getInetAddresses();
            while (e.hasMoreElements()) {
                final InetAddress inetAddress = e.nextElement();
                if (preferIPv4Stack && inetAddress instanceof Inet6Address) {
                    continue;
                }
                if (interfaces != null && !interfaces.isEmpty()) {
                    final AddressDefinition address;
                    if ((address = match(inetAddress, interfaces)) != null) {
                        return address;
                    }
                } else if (!inetAddress.isLoopbackAddress()) {
                    return new AddressDefinition(inetAddress);
                }
            }
        }
        return null;
    }

    private AddressDefinition match(final InetAddress address, final Collection<InterfaceDefinition> interfaces) {
        for (final InterfaceDefinition inf : interfaces) {
            if (AddressUtil.matchInterface(address.getHostAddress(), inf.address)) {
                return new AddressDefinition(inf.host, address);
            }
        }
        return null;
    }

    private boolean preferIPv4Stack() {
        boolean preferIPv4Stack = Boolean.getBoolean("java.net.preferIPv4Stack")
                || node.groupProperties.PREFER_IPv4_STACK.getBoolean();
        // AWS does not support IPv6.
        JoinConfig join = node.getConfig().getNetworkConfig().getJoin();
        AwsConfig awsConfig = join.getAwsConfig();
        boolean awsEnabled = awsConfig != null && awsConfig.isEnabled();
        return preferIPv4Stack || awsEnabled;
    }

    @Deprecated
    public Address getAddress() {
        return getBindAddress();
    }

    @Override
    public Address getBindAddress() {
        return bindAddress;
    }

    @Override
    public Address getPublicAddress() {
        return publicAddress;
    }

    @Override
    public ServerSocketChannel getServerSocketChannel() {
        return serverSocketChannel;
    }

    private void log(Level level, String message) {
        logger.log(level, message);
        node.getSystemLogService().logNode(message);
    }

    private class InterfaceDefinition {
        String host;
        String address;

        private InterfaceDefinition() {
        }

        private InterfaceDefinition(final String address) {
            this.host = null;
            this.address = address;
        }

        private InterfaceDefinition(final String host, final String address) {
            this.host = host;
            this.address = address;
        }

        @Override
        public String toString() {
            return host != null ? (host + "/" + address) : address;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final InterfaceDefinition that = (InterfaceDefinition) o;
            if (address != null ? !address.equals(that.address) : that.address != null) {
                return false;
            }
            if (host != null ? !host.equals(that.host) : that.host != null) {
                return false;
            }
            return true;
        }

        @Override
        public int hashCode() {
            int result = host != null ? host.hashCode() : 0;
            result = 31 * result + (address != null ? address.hashCode() : 0);
            return result;
        }
    }

    private class AddressDefinition extends InterfaceDefinition {
        InetAddress inetAddress;
        int port;

        private AddressDefinition() {
        }

        private AddressDefinition(final InetAddress inetAddress) {
            super(inetAddress.getHostAddress());
            this.inetAddress = inetAddress;
        }

        private AddressDefinition(final String host, final InetAddress inetAddress) {
            super(host, inetAddress.getHostAddress());
            this.inetAddress = inetAddress;
        }

        private AddressDefinition(final String host, final int port, final InetAddress inetAddress) {
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
            if (!super.equals(o)){
                return false;
            }

            AddressDefinition that = (AddressDefinition) o;

            if (port != that.port) {
                return false;
            }
            if (inetAddress != null ? !inetAddress.equals(that.inetAddress) : that.inetAddress != null) {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode() {
            int result = super.hashCode();
            result = 31 * result + (inetAddress != null ? inetAddress.hashCode() : 0);
            result = 31 * result + port;
            return result;
        }
    }
}
