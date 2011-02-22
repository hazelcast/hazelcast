/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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

import com.hazelcast.core.Member;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.logging.ILogger;
import com.hazelcast.monitor.MemberState;

import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.logging.Level;

import static com.hazelcast.nio.IOUtil.newInputStream;
import static com.hazelcast.nio.IOUtil.newOutputStream;

public class ManagementConsoleService implements MembershipListener {

    Queue<ClientHandler> qBuffers = new LinkedBlockingDeque<ClientHandler>(100);
    static int bufferSize = 100000;
    private final FactoryImpl factory;
    private volatile Members members = null;
    private volatile boolean running = true;
    private final DatagramSocket datagramSocket;
    private final SocketReadyServerSocket serverSocket;
    private final UDPListener udpListener;
    private final UDPSender udpSender;
    private final TCPListener tcpListener;
    private final List<ClientHandler> lsClientHandlers = new CopyOnWriteArrayList<ClientHandler>();
    private final ILogger logger;

    public ManagementConsoleService(FactoryImpl factoryImpl) throws Exception {
        this.factory = factoryImpl;
        logger = factory.node.getLogger(ManagementConsoleService.class.getName());
        for (int i = 0; i < 100; i++) {
            qBuffers.offer(new ClientHandler());
        }
        factory.getCluster().addMembershipListener(this);
        updateAddresses();
        int port = factory.getCluster().getLocalMember().getInetSocketAddress().getPort() + 100;
        datagramSocket = new DatagramSocket(port);
        serverSocket = new SocketReadyServerSocket(port);
        udpListener = new UDPListener(datagramSocket);
        udpListener.start();
        udpSender = new UDPSender(datagramSocket);
        udpSender.start();
        tcpListener = new TCPListener(serverSocket);
        tcpListener.start();
        logger.log(Level.INFO, "Hazelcast Management Console started at port " + port + ".");
    }

    public void shutdown() {
        running = false;
        try {
            datagramSocket.close();
            serverSocket.close();
            for (ClientHandler clientHandler : lsClientHandlers) {
                clientHandler.shutdown();
            }
        } catch (Throwable ignored) {
        }
    }

    public void memberAdded(MembershipEvent membershipEvent) {
        updateAddresses();
    }

    public void memberRemoved(MembershipEvent membershipEvent) {
        updateAddresses();
    }

    void updateAddresses() {
        Set<Member> memberSet = new LinkedHashSet<Member>(factory.getCluster().getMembers());
        final Members newMembers = new Members(memberSet.size());
        int i = 0;
        for (Member member : memberSet) {
            MemberImpl memberImpl = (MemberImpl) member;
            SocketAddress sa = new InetSocketAddress(memberImpl.getInetAddress(), memberImpl.getPort() + 100);
            MemberStateImpl memberState = new MemberStateImpl();
            memberState.setMember(memberImpl);
            newMembers.put(i++, sa, memberState);
        }
        members = newMembers;
    }

    class Members {
        private final Map<SocketAddress, MemberState> states = new ConcurrentHashMap(1000);
        private final SocketAddress[] socketAddresses;

        Members(int size) {
            socketAddresses = new SocketAddress[size];
        }

        public void put(int index, SocketAddress socketAddress, MemberState memberState) {
            socketAddresses[index] = socketAddress;
            states.put(socketAddress, memberState);
        }

        public MemberState getMemberState(SocketAddress socketAddress) {
            return states.get(socketAddress);
        }
    }

    class TCPListener extends Thread {
        final SocketReadyServerSocket serverSocket;

        TCPListener(SocketReadyServerSocket serverSocket) {
            this.serverSocket = serverSocket;
        }

        public void run() {
            try {
                while (running) {
                    ClientHandler clientHandler = qBuffers.poll();
                    serverSocket.doAccept(clientHandler.getSocket());
                    clientHandler.start();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    class UDPListener extends Thread {
        final DatagramSocket socket;

        public UDPListener(DatagramSocket socket) throws SocketException {
            super("hz.UDP.Listener");
            this.socket = socket;
            this.socket.setSoTimeout(1000);
        }

        public void run() {
            try {
                final ByteBuffer bbState = ByteBuffer.allocate(1000000);
                final DatagramPacket packet = new DatagramPacket(bbState.array(), bbState.capacity());
                final DataInputStream dis = new DataInputStream(newInputStream(bbState));
                while (running) {
                    try {
                        bbState.clear();
                        socket.receive(packet);
                        MemberState memberState = members.getMemberState(packet.getSocketAddress());
                        if (memberState != null) {
                            memberState.readData(dis);
                        }
                    } catch (SocketTimeoutException ignored) {
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    class UDPSender extends Thread {
        final DatagramSocket socket;
        final DatagramPacket packet = new DatagramPacket(new byte[0], 0);
        final ByteBuffer bbState = ByteBuffer.allocate(1000000);
        final DataOutputStream dos = new DataOutputStream(newOutputStream(bbState));
        final MemberStateImpl state = new MemberStateImpl();

        public UDPSender(DatagramSocket socket) throws SocketException {
            super("hz.UDP.Listener");
            this.socket = socket;
        }

        public void run() {
            try {
                while (running) {
                    sendState();
                    Thread.sleep(5000);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        void sendState() {
            if (members != null) {
                for (SocketAddress address : members.socketAddresses) {
                    try {
                        updateState();
                        bbState.clear();
                        state.writeData(dos);
                        packet.setData(bbState.array(), 0, bbState.position());
                        packet.setSocketAddress(address);
                        socket.send(packet);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        void updateState() {
            factory.createMemberState(state);
        }
    }

    class ClientHandler extends Thread {
        final ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
        final DataOutputStream dos = new DataOutputStream(newOutputStream(buffer));

        final Socket socket = new Socket();

        public ClientHandler() {
        }

        public Socket getSocket() {
            return socket;
        }

        public void run() {
            try {
                InputStream in = socket.getInputStream();
                OutputStream out = socket.getOutputStream();
                while (running) {
                    int request = in.read();
                    buffer.clear();
                    buffer.putInt(0);
                    writeState(members, dos);
                    buffer.putInt(0, buffer.position() - 4);
                    out.write(buffer.array(), 0, buffer.position());
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        public void shutdown() {
            try {
                socket.close();
            } catch (Throwable ignored) {
            }
        }
    }

    static void writeState(final Members members, final DataOutputStream dos) throws Exception {
        dos.writeInt(members.socketAddresses.length);
        for (SocketAddress socketAddress : members.socketAddresses) {
            MemberState memberState = members.getMemberState(socketAddress);
            if (memberState != null) {
                dos.write((byte) 1);
                memberState.writeData(dos);
            } else {
                dos.write((byte) 0);
            }
        }
    }

    public static class SocketReadyServerSocket extends ServerSocket {

        public SocketReadyServerSocket(int port) throws IOException {
            super(port);
        }

        public void doAccept(Socket socket) throws IOException {
            super.implAccept(socket);
        }
    }
}
