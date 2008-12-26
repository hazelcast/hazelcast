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

import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import sun.util.logging.resources.logging;

import com.hazelcast.impl.MulticastService.JoinInfo;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionManager;
import com.hazelcast.nio.InSelector;
import com.hazelcast.nio.OutSelector;

public class Node {

	Address address = null;

	Address masterAddress = null;

	private static Node instance = new Node();

	public Object joinLock = new Object();

	private volatile boolean joined = false;

	static final boolean DEBUG = Build.get().DEBUG;

	private ClusterImpl clusterImpl = null;

	private CoreDump coreDump = new CoreDump();

	private Thread firstMainThread = null;

	private Node() {
	}

	public static Node get() {
		return instance;
	}

	public void restart() {
		shutdown();
		start();
	}

	public void shutdown() {
		MulticastService.get().stop();
		ConnectionManager.get().shutdown();
		ClusterService.get().stop();
		address = null;
		masterAddress = null;
	}

	public ClusterImpl getClusterImpl() {
		return clusterImpl;
	}

	public CoreDump getCoreDump() {
		return coreDump;
	}

	private boolean init() {
		try {
			String preferIPv4Stack = System.getProperty("java.net.preferIPv4Stack");
			String preferIPv6Address = System.getProperty("java.net.preferIPv6Addresses");
			if (preferIPv6Address == null && preferIPv4Stack == null) {
				System.setProperty("java.net.preferIPv4Stack", "true");
			}
			Config config = Config.get();
			ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
			address = AddressPicker.pickAddress(serverSocketChannel);
			InSelector.get().init(serverSocketChannel);
			if (address == null)
				return false;
			System.out.println("Hazelcast " + Build.get().version + " (" + Build.get().build
					+ ") starting at " + address);
			System.out.println("Copyright (C) 2008 Hazelcast.com");

			if (config.join.multicastConfig.enabled) {
				MulticastSocket multicastSocket = new MulticastSocket(null);
				multicastSocket.setReuseAddress(true);
				// bind to receive interface
				multicastSocket.bind(new InetSocketAddress(
						config.join.multicastConfig.multicastPort));
				multicastSocket.setTimeToLive(32);
				// set the send interface
				multicastSocket.setInterface(address.getInetAddress());
				multicastSocket.setReceiveBufferSize(1 * 1024);
				multicastSocket.setSendBufferSize(1 * 1024);
				multicastSocket.joinGroup(InetAddress
						.getByName(config.join.multicastConfig.multicastGroup));
				multicastSocket.setSoTimeout(1000);
				MulticastService.get().init(multicastSocket);
			}

		} catch (Exception e) {
			dumpCore(e);
			e.printStackTrace();
			return false;
		}
		return true;
	}

	private List<Thread> lsThreads = new ArrayList<Thread>(3);

	private BlockingQueue<Address> qFailedConnections = new LinkedBlockingQueue<Address>();

	public void start() {
		firstMainThread = Thread.currentThread();
		clusterImpl = new ClusterImpl();
		boolean inited = init();
		if (!inited)
			return;
		Thread inThread = new Thread(InSelector.get(), "InThread");
		inThread.start();
		inThread.setPriority(8);
		lsThreads.add(inThread);

		Thread outThread = new Thread(OutSelector.get(), "OutThread");
		outThread.start();
		outThread.setPriority(8);
		lsThreads.add(outThread);

		Thread clusterServiceThread = new Thread(ClusterService.get(), "ClusterService");
		clusterServiceThread.start();
		clusterServiceThread.setPriority(7);
		lsThreads.add(clusterServiceThread);

		join();

		if (Config.get().join.multicastConfig.enabled) {
			startMulticastService();
		}
		firstMainThread = null;
	}

	public synchronized void handleInterruptedException(Thread thread, Exception e) {
		PrintWriter pw = coreDump.getPrintWriter();
		pw.write(thread.toString());
		pw.write("\n");
		StackTraceElement[] stEls = e.getStackTrace();
		for (StackTraceElement stackTraceElement : stEls) {
			pw.write("\tat " + stackTraceElement + "\n");
		}
		Throwable cause = e.getCause();
		if (cause != null) {
			pw.write("\tcaused by " + cause);
		}
	}

	public synchronized void exceptionToStringBuffer(Throwable e, StringBuffer sb) {

		StackTraceElement[] stEls = e.getStackTrace();
		for (StackTraceElement stackTraceElement : stEls) {
			sb.append("\tat " + stackTraceElement + "\n");
		}
		Throwable cause = e.getCause();
		if (cause != null) {
			sb.append("\tcaused by " + cause);
		}
	}

	public void dumpCore(Throwable ex) {
		try {
			StringBuffer sb = new StringBuffer();
			if (ex != null) {
				exceptionToStringBuffer(ex, sb);
			}
			sb.append("Hazelcast.version : " + Build.get().version + "\n");
			sb.append("Hazelcast.build   : " + Build.get().build + "\n");
			sb.append("Hazelcast.address   : " + address + "\n");
			sb.append("joined : " + joined + "\n");
			sb.append(AddressPicker.createCoreDump());
			coreDump.getPrintWriter().write(sb.toString());
			coreDump.getPrintWriter().write("\n");
			coreDump.getPrintWriter().write("\n");
			for (Thread thread : lsThreads) {
				thread.interrupt();
			}
			if (!joined) {
				if (firstMainThread != null) {
					try {
						firstMainThread.interrupt();
					} catch (Exception e) {
					}
				}
			}
			String fileName = "hz-core";
			if (address != null)
				fileName += "-" + address.getHost() + "_" + address.getPort();
			fileName += ".txt";
			FileOutputStream fos = new FileOutputStream(fileName);
			Util.writeText(coreDump.toString(), fos);
			fos.flush();
			fos.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void startMulticastService() {
		Thread multicastServiceThread = new Thread(MulticastService.get(), "JoinService");
		multicastServiceThread.start();
		multicastServiceThread.setPriority(6);
	}

	public void unlock() {
		joined = true;
		synchronized (joinLock) {
			joinLock.notify();
		}
	}

	public boolean isMaster(Address address) {
		return (address.equals(masterAddress));
	}

	public boolean joined() {
		return joined;
	}

	public boolean isIP(String address) {
		if (address.indexOf('.') == -1) {
			return false;
		} else {
			StringTokenizer st = new StringTokenizer(address, ".");
			int tokenCount = 0;
			while (st.hasMoreTokens()) {
				String token = st.nextToken();
				tokenCount++;
				try {
					Integer.parseInt(token);
				} catch (Exception e) {
					return false;
				}
			}
			if (tokenCount != 4)
				return false;
		}
		return true;
	}

	private Address getAddressFor(String host) {
		Config config = Config.get();
		int port = config.port;
		int indexColon = host.indexOf(':');
		if (indexColon != -1) {
			port = Integer.parseInt(host.substring(indexColon + 1));
		}
		boolean ip = isIP(host);
		try {
			if (ip) {
				return new Address(host, port, true);
			} else {
				InetAddress[] allAddresses = InetAddress.getAllByName(host);
				for (InetAddress inetAddress : allAddresses) {
					boolean shouldCheck = true;
					Address address = null;
					if (config.interfaces.enabled) {
						address = new Address(inetAddress.getAddress(), config.port);
						shouldCheck = AddressPicker.matchAddress(address.getHost());
					}
					if (shouldCheck) {
						return new Address(inetAddress.getAddress(), port);
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	private List<Address> getPossibleMembers(List<String> lsJoinMembers) {
		Config config = Config.get();
		List<Address> lsPossibleAddresses = new ArrayList<Address>();
		for (String host : lsJoinMembers) {
			// check if host is hostname of ip address
			boolean ip = isIP(host);
			try {
				if (ip) {
					for (int i = 0; i < 3; i++) {
						Address address = new Address(host, config.port + i, true);
						lsPossibleAddresses.add(address);
					}
				} else {
					InetAddress[] allAddresses = InetAddress.getAllByName(host);
					for (InetAddress inetAddress : allAddresses) {
						boolean shouldCheck = true;
						Address address = null;
						if (config.interfaces.enabled) {
							address = new Address(inetAddress.getAddress(), config.port);
							shouldCheck = AddressPicker.matchAddress(address.getHost());
						}
						if (shouldCheck) {
							for (int i = 0; i < 3; i++) {
								Address addressProper = new Address(inetAddress.getAddress(),
										config.port + i);
								lsPossibleAddresses.add(addressProper);
							}
						}
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return lsPossibleAddresses;
	}

	private void joinViaRequiredMember() {
		
		try {
			Config config = Config.get();
			Address requiredAddress = getAddressFor(config.join.joinMembers.requiredMember);
			if (DEBUG) {
				System.out.println("Joining over required member " + requiredAddress);
			}
			if (requiredAddress == null) {
				throw new RuntimeException ("Invalid required member " + config.join.joinMembers.requiredMember);
			}
			if (requiredAddress.equals(address)) {
				setAsMaster();
				return;
			}
			ConnectionManager.get().getOrConnect(requiredAddress);
			Connection conn = null;
			while (conn == null) {
				conn = ConnectionManager.get().getOrConnect(requiredAddress);
				Thread.sleep(1000);				
			}
			while (!joined) {
				Connection connection = ConnectionManager.get().getOrConnect(requiredAddress);
				if (connection == null)
					joinViaRequiredMember();
				if (DEBUG) {
					System.out.println("Sending joinRequest " + requiredAddress);
				}
				ClusterManager.get().sendJoinRequest(requiredAddress);

				Thread.sleep(2000);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void joinWithTCP() {
		Config config = Config.get();
		if (config.join.joinMembers.requiredMember != null) {
			joinViaRequiredMember();
		} else {
			joinViaPossibleMembers();
		}
	}

	private void joinViaPossibleMembers() {
		Config config = Config.get();
		try {
			List<Address> lsPossibleAddresses = getPossibleMembers(config.join.joinMembers.lsMembers);
			lsPossibleAddresses.remove(address);
			for (Address adrs : lsPossibleAddresses) {
				if (DEBUG)
					System.out.println("connecting to " + adrs);
				ConnectionManager.get().getOrConnect(adrs);
			}
			boolean found = false;
			int numberOfSeconds = 0;
			connectionTimeout: while (!found
					&& numberOfSeconds < config.join.joinMembers.connectionTimeoutSeconds) {
				Address addressFailed = null;
				while ((addressFailed = qFailedConnections.poll()) != null) {
					lsPossibleAddresses.remove(addressFailed);
				}
				if (lsPossibleAddresses.size() == 0)
					break connectionTimeout;
				Thread.sleep(1000);
				numberOfSeconds++;
				int numberOfJoinReq = 0;
				for (Address adrs : lsPossibleAddresses) {
					Connection conn = ConnectionManager.get().getOrConnect(adrs);
					if (DEBUG)
						System.out.println("conn " + conn);
					if (conn != null && numberOfJoinReq < 5) {
						found = true;
						ClusterManager.get().sendJoinRequest(adrs);
						numberOfJoinReq++;
					}
				}
			}
			if (DEBUG)
				System.out.println("FOUND " + found);
			if (!found) {
				setAsMaster();
			} else {
				while (!joined) {
					int numberOfJoinReq = 0;
					for (Address adrs : lsPossibleAddresses) {
						Connection conn = ConnectionManager.get().getOrConnect(adrs);
						if (conn != null && numberOfJoinReq < 5) {
							found = true;
							ClusterManager.get().sendJoinRequest(adrs);
							numberOfJoinReq++;
						}
					}
					Thread.sleep(2000);
					if (DEBUG) {
						System.out.println(masterAddress);
					}
					if (masterAddress == null) { // no-one knows the master
						boolean masterCandidate = true;
						for (Address address : lsPossibleAddresses) {
							if (this.address.hashCode() > address.hashCode())
								masterCandidate = false;
						}
						if (masterCandidate) {
							setAsMaster();
						}
					}
				}

			}
			lsPossibleAddresses.clear();
			qFailedConnections.clear();
		} catch (Exception e) {
			e.printStackTrace();
		}
		if (DEBUG)
			System.out.println("DONE TCP");
		StringBuilder sb = new StringBuilder();
		sb.append("\n");
		if (ClusterService.get().getMemberCount() == 1)
			sb.append(ClusterManager.get());
		System.out.println(sb.toString());
	}

	void setAsMaster () {
		masterAddress = address;
		if (DEBUG)
			System.out.println("adding member myself");
		ClusterService.get().addMember(address); // add
		// myself
		clusterImpl.setMembers(ClusterService.get().lsMembers);
		unlock();
	}
	public void reJoin() {
		System.out.println("REJOINING...");
		joined = false;
		masterAddress = null;
		join();
	}

	private void join() {
		Config config = Config.get();
		if (!config.join.multicastConfig.enabled) {
			joinWithTCP();
			return;
		}
		masterAddress = findMaster();
		if (DEBUG)
			System.out.println(address + " master: " + masterAddress);
		if (masterAddress == null || masterAddress.equals(address)) {
			ClusterService.get().addMember(address); // add myself
			masterAddress = address;
			clusterImpl.setMembers(ClusterService.get().lsMembers);
			unlock();
		} else {
			while (!joined) {
				try {
					if (DEBUG)
						System.out.println("joining... " + masterAddress);
					synchronized (joinLock) {
						joinExisting(masterAddress);
						joinLock.wait(2000);
					}
					if (masterAddress == null) {
						join();
					} else if (masterAddress.equals(address)) {
						setAsMaster();
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}

		if (DEBUG)
			System.out.println("Join DONE");
		StringBuilder sb = new StringBuilder();
		sb.append("\n");
		if (ClusterService.get().getMemberCount() == 1)
			sb.append(ClusterManager.get());
		System.out.println(sb.toString());

	}

	private void joinExisting(Address masterAddress) throws Exception {
		Connection conn = ConnectionManager.get().getOrConnect(masterAddress);
		if (conn == null)
			Thread.sleep(1000);
		conn = ConnectionManager.get().getConnection(masterAddress);
		if (DEBUG) {
			System.out.println("Master connnection " + conn);
		}
		if (conn != null)
			ClusterManager.get().sendJoinRequest(masterAddress);
	}

	private Address findMaster() {
		Config config = Config.get();
		try {
			String ip = System.getProperty("join.ip");
			if (ip == null) {
				JoinInfo joinInfo = new JoinInfo(true, address, config.groupName,
						config.groupPassword, config.groupMembershipType);
				for (int i = 0; i < 5; i++) {
					MulticastService.get().send(joinInfo);
					Thread.sleep(10);
				}
				JoinInfo respJoinInfo = null;
				boolean timedOut = false;
				while (!timedOut) {
					respJoinInfo = MulticastService.get().receive();
					if (respJoinInfo == null) {
						timedOut = true;
					} else if (!respJoinInfo.request && !respJoinInfo.address.equals(address)) {
						timedOut = true;
					}
				}
				if (respJoinInfo != null) {
					masterAddress = respJoinInfo.address;
					return masterAddress;
				} else {
					joinInfo = new JoinInfo(false, address, config.groupName, config.groupPassword,
							config.groupMembershipType);
					for (int i = 0; i < 5; i++) {
						MulticastService.get().send(joinInfo);
					}
					return address;
				}

			} else {
				if (DEBUG)
					System.out.println("RETURNING join.ip");
				return new Address(ip, config.port);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	public Address getThisAddress() {
		return address;
	}

	public boolean master() {
		return address.equals(masterAddress);
	}

	public Address getMasterAddress() {
		return masterAddress;
	}

	public void setMasterAddress(Address master) {
		masterAddress = master;
	}

	public static void main(String[] args) {
		try {
			int port = 5701;
			InetAddress addr = InetAddress
					.getByAddress(new byte[] { (byte) 192, (byte) 168, 1, 2 });
			SocketAddress sockaddr = new InetSocketAddress(addr, port);

			// Create an unbound socket
			Socket sock = new Socket();

			// This method will block no more than timeoutMs.
			// If the timeout occurs, SocketTimeoutException is thrown.
			int timeoutMs = 2000; // 2 seconds
			sock.connect(sockaddr, timeoutMs);

			System.in.read();
			sock.close();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void failedConnection(Address address) {
		qFailedConnections.offer(address);
	}

}
