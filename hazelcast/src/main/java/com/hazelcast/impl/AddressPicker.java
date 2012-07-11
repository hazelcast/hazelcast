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
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Address;
import com.hazelcast.util.AddressUtil;

import java.net.*;
import java.nio.channels.ServerSocketChannel;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashSet;
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
            final InetAddress inetAddress = pickInetAddress(config);
            final boolean reuseAddress = config.isReuseAddress();
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
                    boolean bindAny = node.getGroupProperties().SOCKET_BIND_ANY.getBoolean();
                    if (bindAny) {
                        isa = new InetSocketAddress(port);
                    } else {
                        isa = new InetSocketAddress(inetAddress, port);
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
            address = new Address(inetAddress, port);
        } catch (Exception e) {
            logger.log(Level.SEVERE, e.getMessage(), e);
            throw e;
        }
    }

    private InetAddress pickInetAddress(final Config config) throws UnknownHostException, SocketException {
        InetAddress currentInetAddress = getSystemConfiguredAddress();
        if (currentInetAddress == null) {
            final Collection<String> interfaces = getInterfaces(config);
            if (interfaces.contains("127.0.0.1") || interfaces.contains("localhost")) {
                currentInetAddress = pickLoopbackAddress();
            } else {
                if (interfaces.size() > 0) {
                    currentInetAddress = pickInetAddress(interfaces);
                }
                if (currentInetAddress == null) {
                    if (config.getNetworkConfig().getInterfaces().isEnabled()) {
                        String msg = "Hazelcast CANNOT start on this node. No matching network interface found. ";
                        msg += "\nInterface matching must be either disabled or updated in the hazelcast.xml config file.";
                        logger.log(Level.SEVERE, msg);
                        throw new RuntimeException(msg);
                    } else {
                        currentInetAddress = pickInetAddress((Collection<String>) null);
                    }
                }
            }
        }
        if (currentInetAddress != null) {
            // check if scope id correctly set
            currentInetAddress = AddressUtil.fixAndGetInetAddress(currentInetAddress);
        }
        if (currentInetAddress == null) {
            currentInetAddress = pickLoopbackAddress();
        }
        return currentInetAddress;
    }

    private Collection<String> getInterfaces(final Config config) {
        final Collection<String> interfaces = new HashSet<String>();
        if (config.getNetworkConfig().getInterfaces().isEnabled()) {
            interfaces.addAll(config.getNetworkConfig().getInterfaces().getInterfaces());
            logger.log(Level.INFO, "Interfaces is enabled, trying to pick one address matching " +
                                     "to one of: " + interfaces);
        }
        else if (config.getNetworkConfig().getJoin().getTcpIpConfig().isEnabled()) {
            final Collection<String> possibleAddresses = TcpIpJoiner.getConfigurationMembers(node.config);
            for (String possibleAddress : possibleAddresses) {
                interfaces.add(AddressUtil.getAddressHolder(possibleAddress).address);
            }
            logger.log(Level.FINEST, "Interfaces is disabled, trying to pick one address from TCP-IP config " +
                                     "addresses: " + interfaces);
        }
        return interfaces;
    }

    private InetAddress getSystemConfiguredAddress() throws UnknownHostException {
        final String localAddress = System.getProperty("hazelcast.local.localAddress");
        if (localAddress != null) {
            if ("127.0.0.1".equals(localAddress.trim()) || "localhost".equals(localAddress.trim())) {
                return pickLoopbackAddress();
            } else {
                return InetAddress.getByName(localAddress.trim());
            }
        }
        return null;
    }

    private InetAddress pickLoopbackAddress() throws UnknownHostException {
        if (System.getProperty("java.net.preferIPv6Addresses") == null
                && System.getProperty("java.net.preferIPv4Stack") == null) {
            // When using loopback address and multicast join, leaving IPv6 enabled causes join issues.
            logger.log(Level.WARNING, "Picking loopback address [127.0.0.1]; setting 'java.net.preferIPv4Stack' to true.");
            System.setProperty("java.net.preferIPv4Stack", "true");
        }
        return InetAddress.getByName("127.0.0.1");
    }

    private InetAddress pickInetAddress(final Collection<String> interfaces) throws SocketException {
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
                    final String address = inetAddress.getHostAddress();
                    if (AddressUtil.matchAnyInterface(address, interfaces)) {
                        return inetAddress;
                    }
                } else if (!inetAddress.isLoopbackAddress()) {
                    return inetAddress;
                }
            }
        }
        return null;
    }

    private boolean preferIPv4Stack() {
        boolean preferIPv4Stack = node.groupProperties.PREFER_IPv4_STACK.getBoolean();
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
}
