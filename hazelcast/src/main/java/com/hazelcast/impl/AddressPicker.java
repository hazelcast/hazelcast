/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

package com.hazelcast.impl;

import com.hazelcast.config.AwsConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.Join;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Address;
import com.hazelcast.util.AddressUtil;

import java.net.*;
import java.nio.channels.ServerSocketChannel;
import java.util.*;
import java.util.logging.Level;

class AddressPicker {

    private final Node node;
    private final ILogger logger;
    private ServerSocketChannel serverSocketChannel;
    private Address address;

    public AddressPicker(Node node) {
        this.node = node;
        this.logger = Logger.getLogger(AddressPicker.class.getName());
    }

    public void pickAddress() throws Exception {
        if (address != null) {
            return;
        }
        try {
            final Config config = node.getConfig();
            final AddressDefinition addressDef = pickAddress(config);
            final boolean reuseAddress = config.isReuseAddress();
            final boolean bindAny = node.getGroupProperties().SOCKET_BIND_ANY.getBoolean();
            serverSocketChannel = ServerSocketChannel.open();
            final ServerSocket serverSocket = serverSocketChannel.socket();
            /**
             * why setReuseAddress(true)?
             * when the member is shutdown,
             * the serversocket port will be in TIME_WAIT state for the next
             * 2 minutes or so. If you start the member right after shutting it down
             * you may not able able to bind to the same port because it is in TIME_WAIT
             * state. if you set reuseaddress=true then TIME_WAIT will be ignored and
             * you will be able to bind to the same port again.
             *
             * this will cause problem on windows
             * see http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6421091
             * http://www.hsc.fr/ressources/articles/win_net_srv/multiple_bindings.html
             *
             * By default if the OS is Windows then reuseaddress will be false.
             */
            log(Level.FINEST, "inet reuseAddress:" + reuseAddress);
            serverSocket.setReuseAddress(reuseAddress);
            serverSocket.setSoTimeout(1000);
            InetSocketAddress isa;
            int port = config.getPort();
            for (int i = 0; i < 100; i++) {
                try {
                    if (bindAny) {
                        isa = new InetSocketAddress(port);
                    } else {
                        isa = new InetSocketAddress(addressDef.inetAddress, port);
                    }
                    log(Level.FINEST, "Trying to bind inet socket address:" + isa);
                    serverSocket.bind(isa, 100);
                    log(Level.FINEST, "Bind successful to inet socket address:" + isa);
                    break;
                } catch (final Exception e) {
                    if (config.isPortAutoIncrement()) {
                        port++;
                    } else {
                        String msg = "Port [" + port + "] is already in use and auto-increment is " +
                                     "disabled. Hazelcast cannot start.";
                        logger.log(Level.SEVERE, msg, e);
                        throw e;
                    }
                }
            }
            serverSocketChannel.configureBlocking(false);
            address = new Address(addressDef.host, port);
            logger.log(Level.INFO,
                    "Picked " + address + ", using socket " + serverSocket + ", bind any local is " + bindAny);
        } catch (Exception e) {
            logger.log(Level.SEVERE, e.getMessage(), e);
            throw e;
        }
    }

    private AddressDefinition pickAddress(final Config config) throws UnknownHostException, SocketException {
        AddressDefinition infAddress = getSystemConfiguredAddress();
        if (infAddress == null) {
            final NetworkConfig networkConfig = config.getNetworkConfig();
            final Collection<InterfaceDefinition> interfaces = getInterfaces(networkConfig);
            if (interfaces.contains(new InterfaceDefinition("127.0.0.1"))
                    || interfaces.contains(new InterfaceDefinition("localhost"))) {
                infAddress = pickLoopbackAddress();
            } else {
                if (preferIPv4Stack()) {
                    logger.log(Level.INFO, "Prefer IPv4 stack is true.");
                }
                if (interfaces.size() > 0) {
                    infAddress = pickMatchingAddress(interfaces);
                }
                if (infAddress == null) {
                    if (networkConfig.getInterfaces().isEnabled()) {
                        String msg = "Hazelcast CANNOT start on this node. No matching network interface found. ";
                        msg += "\nInterface matching must be either disabled or updated in the hazelcast.xml config file.";
                        logger.log(Level.SEVERE, msg);
                        throw new RuntimeException(msg);
                    } else {
                        if (networkConfig.getJoin().getTcpIpConfig().isEnabled()) {
                            logger.log(Level.WARNING, "Could not find a matching address to start with! " +
                                                      "Picking one of non-loopback addresses.");
                        }
                        infAddress = pickMatchingAddress(null);
                    }
                }
            }
        }
        if (infAddress != null) {
            // check if scope id correctly set
            infAddress.inetAddress = AddressUtil.fixScopeIdAndGetInetAddress(infAddress.inetAddress);
        }
        if (infAddress == null) {
            infAddress = pickLoopbackAddress();
        }
        return infAddress;
    }

    private Collection<InterfaceDefinition> getInterfaces(final NetworkConfig networkConfig) throws UnknownHostException {
        final Collection<InterfaceDefinition> interfaces = new HashSet<InterfaceDefinition>();
        if (networkConfig.getInterfaces().isEnabled()) {
            final Collection<String> configInterfaces = networkConfig.getInterfaces().getInterfaces();
            for (String configInterface : configInterfaces) {
                if (AddressUtil.isIpAddress(configInterface)) {
                    interfaces.add(new InterfaceDefinition(configInterface));
                } else {
                    logger.log(Level.INFO, "'" + configInterface
                                           + "' is not an IP address! Removing from interface list.");
                }
            }
            logger.log(Level.INFO, "Interfaces is enabled, trying to pick one address matching " +
                                   "to one of: " + interfaces);
        } else if (networkConfig.getJoin().getTcpIpConfig().isEnabled()) {
            final Collection<String> possibleAddresses = TcpIpJoiner.getConfigurationMembers(node.config);
            for (String possibleAddress : possibleAddresses) {
                final String s = AddressUtil.getAddressHolder(possibleAddress).address;
                if (AddressUtil.isIpAddress(s)) {
                    interfaces.add(new InterfaceDefinition(s));
                } else {
                    String address = resolveDomainName(s);
                    logger.log(Level.INFO, "Updating interface list with " + address + " for domain name '" + s + "'.");
                    interfaces.add(new InterfaceDefinition(s, address));
                }
            }
            logger.log(Level.INFO, "Interfaces is disabled, trying to pick one address from TCP-IP config " +
                                   "addresses: " + interfaces);
        }
        return interfaces;
    }

    private String resolveDomainName(final String domainName)
            throws UnknownHostException {
        final InetAddress[] inetAddresses = InetAddress.getAllByName(domainName);
        if (inetAddresses.length > 1) {
            logger.log(Level.WARNING, "Domain name '" + domainName + "' resolves to more than one address: " +
                                      Arrays.toString(inetAddresses) + "! Hazelcast will use the first one.");
        }
        final InetAddress inetAddress = inetAddresses[0];
        return inetAddress.getHostAddress();
    }

    private AddressDefinition getSystemConfiguredAddress() throws UnknownHostException {
        String localAddress = System.getProperty("hazelcast.local.localAddress");
        if (localAddress != null) {
            localAddress = localAddress.trim();
            if ("127.0.0.1".equals(localAddress) || "localhost".equals(localAddress)) {
                return pickLoopbackAddress();
            } else {
                logger.log(Level.INFO, "Picking address configured by System property 'hazelcast.local.localAddress'");
                return new AddressDefinition(localAddress, InetAddress.getByName(localAddress));
            }
        }
        return null;
    }

    // http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6402758
    private AddressDefinition pickLoopbackAddress() throws UnknownHostException {
        if (System.getProperty("java.net.preferIPv6Addresses") == null
            && System.getProperty("java.net.preferIPv4Stack") == null) {
            // When using loopback address and multicast join, leaving IPv6 enabled causes join issues.
            logger.log(Level.WARNING,
                    "Picking loopback address [127.0.0.1]; setting 'java.net.preferIPv4Stack' to true.");
            System.setProperty("java.net.preferIPv4Stack", "true");
        }
        return new AddressDefinition("127.0.0.1", InetAddress.getByName("127.0.0.1"));
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
                    final AddressDefinition address ;
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
        Join join = node.getConfig().getNetworkConfig().getJoin();
        AwsConfig awsConfig = join.getAwsConfig();
        boolean awsEnabled = awsConfig != null && awsConfig.isEnabled();
        return preferIPv4Stack || awsEnabled;
    }

    public Address getAddress() {
        return address;
    }

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
            this.host = address;
            this.address = address;
        }

        private InterfaceDefinition(final String host, final String address) {
            this.host = host;
            this.address = address;
        }

        @Override
        public String toString() {
            return host;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            final InterfaceDefinition that = (InterfaceDefinition) o;

            if (address != null ? !address.equals(that.address) : that.address != null) return false;
            if (host != null ? !host.equals(that.host) : that.host != null) return false;

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
    }
}
