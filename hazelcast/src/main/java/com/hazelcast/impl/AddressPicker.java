/* 
 * Copyright (c) 2007-2008, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.impl;

import com.hazelcast.config.Config;
import com.hazelcast.nio.Address;

import java.net.*;
import java.nio.channels.ServerSocketChannel;
import java.util.Enumeration;
import java.util.List;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;

public class AddressPicker {
    protected static Logger logger = Logger.getLogger(AddressPicker.class.getName());

    public AddressPicker() {
    }

    public static boolean matchAddress(final String address, final List<String> interfaces) {
        final int[] ip = new int[4];
        int i = 0;
        final StringTokenizer st = new StringTokenizer(address, ".");
        while (st.hasMoreTokens()) {
            ip[i++] = Integer.parseInt(st.nextToken());
        }
        for (final String ipmask : interfaces) {
            if (matchAddress(ipmask, ip)) {
                return true;
            }
        }
        return false;
    }

    private static boolean matchAddress(final String ipmask, final int[] ip) {
        final String[] ips = new String[4];
        final StringTokenizer st = new StringTokenizer(ipmask, ".");
        int i = 0;
        while (st.hasMoreTokens()) {
            ips[i++] = st.nextToken();
        }
        for (int a = 0; a < 4; a++) {
            final String mask = ips[a];
            final int ipa = ip[a];
            final int dashIndex = mask.indexOf('-');
            if (mask.equals("*")) {
            } else if (dashIndex != -1) {
                final int start = Integer.parseInt(mask.substring(0, dashIndex).trim());
                final int end = Integer.parseInt(mask.substring(dashIndex + 1).trim());
                if (ipa < start || ipa > end)
                    return false;
            } else {
                final int x = Integer.parseInt(mask);
                if (x != ipa)
                    return false;
            }
        }
        return true;
    }

    public Address pickAddress(Node node, final ServerSocketChannel serverSocketChannel)
            throws Exception {
        String currentAddress = null;
        try {
            final Config config = node.getConfig();
            final String localAddress = System.getProperty("hazelcast.local.address");
            if (localAddress != null) {
                currentAddress = InetAddress.getByName(localAddress.trim()).getHostAddress();
            }
            if (currentAddress == null) {
                final Enumeration<NetworkInterface> enums = NetworkInterface.getNetworkInterfaces();
                interfaces:
                while (enums.hasMoreElements()) {
                    final NetworkInterface ni = enums.nextElement();
                    final Enumeration<InetAddress> e = ni.getInetAddresses();
//					final boolean isUp = invoke(true, 1.6, ni, "isUp");     
//					final boolean supportsMulticast = invoke(true, 1.6, ni, "supportsMulticast");
                    while (e.hasMoreElements()) {
                        final InetAddress inetAddress = e.nextElement();
                        if (inetAddress instanceof Inet4Address) {
                            final String address = inetAddress.getHostAddress();
                            final List<String> interfaces = config.getNetworkConfig().getInterfaces().getInterfaceList();
                            if (config.getNetworkConfig().getInterfaces().isEnabled()) {
                                if (matchAddress(address, interfaces)) {
                                    currentAddress = address;
                                    break interfaces;
                                }
                            } else {
                                if (!inetAddress.isLoopbackAddress()) {
                                    currentAddress = address;
                                    break interfaces;
                                }
                            }
                        }
                    }
                }
                if (config.getNetworkConfig().getInterfaces().isEnabled() && currentAddress == null) {
                    String msg = "Hazelcast CANNOT start on this node. No matching network interface found. ";
                    msg += "\nInterface matching must be either disabled or updated in the hazelcast.xml config file.";
                    logger.log(Level.SEVERE, msg);
                    throw new RuntimeException(msg);
                }
            }
            if (currentAddress == null) {
                currentAddress = "127.0.0.1";
            }
            final InetAddress inetAddress = InetAddress.getByName(currentAddress);
            final boolean reuseAddress = config.isReuseAddress();
            ServerSocket serverSocket = serverSocketChannel.socket();
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
            serverSocket.setReuseAddress(reuseAddress);
            InetSocketAddress isa;
            int port = config.getPort();
            for (int i = 0; i < 100; i++) {
                try {
                    isa = new InetSocketAddress(inetAddress, port);
                    serverSocket.bind(isa, 100);
                    break;
                } catch (final Exception e) {
                    if (config.isPortAutoIncrement()) {
                        serverSocket = serverSocketChannel.socket();
                        serverSocket.setReuseAddress(reuseAddress);
                        port++;
                    } else {
                        String msg = "Port [" + port + "] is already in use and auto-increment is " +
                                "disabled. Hazelcast cannot start.";
                        logger.log(Level.SEVERE, msg);
                        throw e;
                    }
                }
            }
            serverSocketChannel.configureBlocking(false);
            return new Address(currentAddress, port);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }
}
