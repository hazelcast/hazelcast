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

import com.hazelcast.nio.Address;

public class AddressPicker {
	static double jvmVersion = 1.5;
	static {
		jvmVersion = Double.parseDouble(System.getProperty("java.vm.version").substring(0, 3));
	}

	public static double getJVMVersion() {
		return jvmVersion;
	}

	public static Address pickAddress(ServerSocketChannel serverSocketChannel) throws Exception {
		String currentAddress = null;
		try {
			Config config = Config.get();
			String localAddress = System.getProperty("the.local.address");

			currentAddress = InetAddress.getByName(localAddress).getHostAddress().trim();
			if (currentAddress == null || currentAddress.length() == 0
					|| currentAddress.equalsIgnoreCase("localhost")
					|| currentAddress.equals("127.0.0.1")) {
				boolean matchFound = false;
				Enumeration<NetworkInterface> enums = NetworkInterface.getNetworkInterfaces();
				interfaces: while (enums.hasMoreElements()) {
					NetworkInterface ni = enums.nextElement();
					Enumeration<InetAddress> e = ni.getInetAddresses();
					boolean isUp = invoke(true, 1.6, ni, "isUp");
					boolean supportsMulticast = invoke(true, 1.6, ni, "supportsMulticast");
					while (e.hasMoreElements()) {
						InetAddress inetAddress = e.nextElement();
						if (inetAddress instanceof Inet4Address) {
							byte[] ip = inetAddress.getAddress();
							String address = inetAddress.getHostAddress();
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
					Util.logFatal(msg);
					Node.get().dumpCore(null);
					return null;
				}
			}

			InetAddress inetAddress = InetAddress.getByName(currentAddress);
			ServerSocket serverSocket = serverSocketChannel.socket();
			serverSocket.setReuseAddress(false);
			InetSocketAddress isa = null;

			int port = config.port;
			socket: for (int i = 0; i < 100; i++) {
				try {
					isa = new InetSocketAddress(inetAddress, port);
					serverSocket.bind(isa, 100);
					break socket;
				} catch (Exception e) {
					serverSocket = serverSocketChannel.socket();
					serverSocket.setReuseAddress(false);
					port++;
					continue socket;
				}
			}
			serverSocketChannel.configureBlocking(false);
			Address selectedAddress = new Address(currentAddress, port);
			return selectedAddress;
		} catch (Exception e) {
			Node.get().dumpCore(e);
			e.printStackTrace();
			throw e;
		}
	}

	public static boolean invoke(boolean defaultValue, double minJVMVersion, NetworkInterface ni,
			String methodName) {
		boolean result = defaultValue;
		if (jvmVersion >= minJVMVersion) {
			try {
				Method method = ni.getClass().getMethod(methodName, null);
				Boolean obj = (Boolean) method.invoke(ni, null);
				result = obj.booleanValue();
			} catch (Exception e) {
			}
		}
		return result;
	}

	public static String createCoreDump() {
		StringBuilder sb = new StringBuilder();
		addLine(sb, "== Config ==");
		addLine(sb, "config url: " + Config.get().urlConfig);
		addLine(sb, Config.get().xmlConfig);
		Set<Object> propKeys = System.getProperties().keySet();
		addLine(sb, "== System Properies ==");
		for (Object key : propKeys) {
			addLine(sb, key + " : " + System.getProperty((String) key));
		}
		try {
			Enumeration<NetworkInterface> enums = NetworkInterface.getNetworkInterfaces();
			while (enums.hasMoreElements()) {
				NetworkInterface ni = enums.nextElement();
				sb.append("\n");
				addLine(sb, "== Interface [" + ni.getName() + "] ==");
				boolean isUp = invoke(true, 1.6, ni, "isUp");
				boolean supportsMulticast = invoke(true, 1.6, ni, "supportsMulticast");
				addLine(sb, "displayName : " + ni.getDisplayName());
				addLine(sb, "isUp : " + isUp);
				addLine(sb, "supportsMulticast : " + supportsMulticast);
				Enumeration<InetAddress> e = ni.getInetAddresses();
				while (e.hasMoreElements()) {
					try {
						InetAddress inetAddress = e.nextElement();
						addLine(1, sb, "-----IP-----");
						boolean ipv4 = (inetAddress instanceof Inet4Address);
						byte[] ip = inetAddress.getAddress();
						String address = inetAddress.getHostAddress();
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
					} catch (Exception ex) {
						addLine(1, sb, "Got Exception: " + ex.getMessage());
					}
				}
			}
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		return sb.toString();
	}

	public static void addLine(StringBuilder sb, String str) {
		addLine(0, sb, str);
	}

	public static void addLine(int tabCount, StringBuilder sb, String str) {
		for (int i = 0; i < tabCount; i++) {
			sb.append("\t");
		}
		sb.append(str);
		sb.append("\n");
	}

	public static boolean matchAddress(String address) {
		int[] ip = new int[4];
		int i = 0;
		StringTokenizer st = new StringTokenizer(address, ".");
		while (st.hasMoreTokens()) {
			ip[i++] = Integer.parseInt(st.nextToken());
		}
		List<String> interfaces = Config.get().interfaces.lsInterfaces;
		for (String ipmask : interfaces) {
			if (matchAddress(ipmask, ip)) {
				return true;
			}
		}
		return false;
	}

	public static boolean matchAddress(String ipmask, int[] ip) {
		String[] ips = new String[4];
		StringTokenizer st = new StringTokenizer(ipmask, ".");
		int i = 0;
		while (st.hasMoreTokens()) {
			ips[i++] = st.nextToken();
		}
		for (int a = 0; a < 4; a++) {
			String mask = ips[a];
			int ipa = ip[a];
			int dashIndex = mask.indexOf('-');
			if (mask.equals("*")) {
			} else if (dashIndex != -1) {
				int start = Integer.parseInt(mask.substring(0, dashIndex).trim());
				int end = Integer.parseInt(mask.substring(dashIndex + 1).trim());
				// System.out.println(start + " start end " + end);
				if (ipa < start || ipa > end)
					return false;
			} else {
				int x = Integer.parseInt(mask);
				if (x != ipa)
					return false;
			}
		}
		return true;
	}
}
