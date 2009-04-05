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

import java.lang.reflect.Method;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.nio.channels.ServerSocketChannel;
import java.util.Enumeration;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.hazelcast.nio.Address;

public class AddressPicker {
    protected static Logger logger = Logger.getLogger(AddressPicker.class.getName());

    static double jvmVersion = 1.5;

    static {
        jvmVersion = Double.parseDouble(System.getProperty("java.version").substring(0, 3));
    }

    public static void addLine(final int tabCount, final StringBuilder sb, final String str) {
        for (int i = 0; i < tabCount; i++) {
            sb.append("\t");
        }
        sb.append(str);
        sb.append("\n");
    }

    public static void addLine(final StringBuilder sb, final String str) {
        addLine(0, sb, str);
    }

    public static String createCoreDump() {
        final StringBuilder sb = new StringBuilder();
        addLine(sb, "== Config ==");
        addLine(sb, "config url: " + Config.get().urlConfig);
        addLine(sb, Config.get().xmlConfig);
        final Set<Object> propKeys = System.getProperties().keySet();
        addLine(sb, "== System Properies ==");
        for (final Object key : propKeys) {
            addLine(sb, key + " : " + System.getProperty((String) key));
        }
        try {
            final Enumeration<NetworkInterface> enums = NetworkInterface.getNetworkInterfaces();
            while (enums.hasMoreElements()) {
                final NetworkInterface ni = enums.nextElement();
                sb.append("\n");
                addLine(sb, "== Interface [" + ni.getName() + "] ==");
                final boolean isUp = invoke(true, 1.6, ni, "isUp");
                final boolean supportsMulticast = invoke(true, 1.6, ni, "supportsMulticast");
                addLine(sb, "displayName : " + ni.getDisplayName());
                addLine(sb, "isUp : " + isUp);
                addLine(sb, "supportsMulticast : " + supportsMulticast);
                final Enumeration<InetAddress> e = ni.getInetAddresses();
                while (e.hasMoreElements()) {
                    try {
                        final InetAddress inetAddress = e.nextElement();
                        addLine(1, sb, "-----IP-----");
                        final boolean ipv4 = (inetAddress instanceof Inet4Address);
                        final String address = inetAddress.getHostAddress();
                        addLine(1, sb, "InetAddress : " + inetAddress);
                        addLine(1, sb, "IP : " + address);
                        addLine(1, sb, "IPv4 : " + ipv4);
                        if (ipv4) {
                            addLine(1, sb, "Address : " + new Address(address, -1, true));
                        }
                        addLine(1, sb, "multicast : " + inetAddress.isMulticastAddress());
                        addLine(1, sb, "loopback : " + inetAddress.isLoopbackAddress());
                        if (Config.get().interfaces.enabled) {
                            addLine(1, sb, "has match : " + matchAddress(address));
                        }
                    } catch (final Exception ex) {
                        addLine(1, sb, "Got Exception: " + ex.getMessage());
                    }
                }
            }
        } catch (final Exception e1) {
            e1.printStackTrace();
        }
        return sb.toString();
    }

    public static double getJVMVersion() {
        return jvmVersion;
    }

    public static boolean invoke(final boolean defaultValue, final double minJVMVersion,
                                 final NetworkInterface ni, final String methodName) {
        boolean result = defaultValue;
        if (jvmVersion >= minJVMVersion) {
            try {
                final Method method = ni.getClass().getMethod(methodName, null);
                final Boolean obj = (Boolean) method.invoke(ni, null);
                result = obj.booleanValue();
            } catch (final Exception e) {
            }
        }
        return result;
    }

    public static boolean matchAddress(final String address) {
        final int[] ip = new int[4];
        int i = 0;
        final StringTokenizer st = new StringTokenizer(address, ".");
        while (st.hasMoreTokens()) {
            ip[i++] = Integer.parseInt(st.nextToken());
        }
        final List<String> interfaces = Config.get().interfaces.lsInterfaces;
        for (final String ipmask : interfaces) {
            if (matchAddress(ipmask, ip)) {
                return true;
            }
        }
        return false;
    }

    public static boolean matchAddress(final String ipmask, final int[] ip) {
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

    public static Address pickAddress(final ServerSocketChannel serverSocketChannel)
            throws Exception {
        String currentAddress = null;
        try {
            final Config config = Config.get();
            final String localAddress = System.getProperty("the.local.address");

            currentAddress = InetAddress.getByName(localAddress).getHostAddress().trim();
            if (currentAddress == null || currentAddress.length() == 0
                    || currentAddress.equalsIgnoreCase("localhost")
                    || currentAddress.equals("127.0.0.1")) {
                boolean matchFound = false;
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
                            if (!inetAddress.isLoopbackAddress()) {
                                currentAddress = address;
                                if (config.interfaces.enabled) {
                                    if (matchAddress(address)) {
                                        matchFound = true;
                                        break interfaces;
                                    }
                                } else {
                                    break interfaces;
                                }
                            }
                        }
                    }
                }
                if (config.interfaces.enabled && !matchFound) {
                    String msg = "Hazelcast CANNOT start on this node. No matching network interface found. ";
                    msg += "\nInterface matching must be either disabled or updated in the hazelcast.xml config file.";
                    logger.log(Level.SEVERE, msg);
                    Node.get().dumpCore(null);
                    return null;
                }
            }

            final InetAddress inetAddress = InetAddress.getByName(currentAddress);
            ServerSocket serverSocket = serverSocketChannel.socket();
            serverSocket.setReuseAddress(false);
            InetSocketAddress isa = null;

            int port = config.port;
            socket:
            for (int i = 0; i < 100; i++) {
                try {
                    isa = new InetSocketAddress(inetAddress, port);
                    serverSocket.bind(isa, 100);
                    break socket;
                } catch (final Exception e) {
                    serverSocket = serverSocketChannel.socket();
                    serverSocket.setReuseAddress(false);
                    port++;
                    continue socket;
                }
            }
            serverSocketChannel.configureBlocking(false);
            final Address selectedAddress = new Address(currentAddress, port);
            return selectedAddress;
        } catch (final Exception e) {
            Node.get().dumpCore(e);
            e.printStackTrace();
            throw e;
        }
    }
}
