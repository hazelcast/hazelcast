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

import com.hazelcast.core.*;
import com.hazelcast.logging.ILogger;
import com.hazelcast.monitor.MemberState;
import com.hazelcast.monitor.TimedClusterState;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.DataSerializable;

import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Level;

import static com.hazelcast.nio.IOUtil.newInputStream;
import static com.hazelcast.nio.IOUtil.newOutputStream;

public class ManagementConsoleService implements MembershipListener {

    public static final byte STATE_OUT_OF_MEMORY = 0;
    public static final byte STATE_ACTIVE = 1;
    public static final int REQUEST_TYPE_CLUSTER_STATE = 1;
    public static final int REQUEST_TYPE_GET_THREAD_DUMP = 2;

    final Queue<ClientHandler> qClientHandlers = new LinkedBlockingQueue<ClientHandler>(100);
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
    private final MemberStateImpl localState = new MemberStateImpl();
    private final SocketAddress localSocketAddress;

    public ManagementConsoleService(FactoryImpl factoryImpl) throws Exception {
        this.factory = factoryImpl;
        logger = factory.node.getLogger(ManagementConsoleService.class.getName());
        for (int i = 0; i < 100; i++) {
            qClientHandlers.offer(new ClientHandler());
        }
        factory.getCluster().addMembershipListener(this);
        MemberImpl memberLocal = (MemberImpl) factory.getCluster().getLocalMember();
        int port = memberLocal.getInetSocketAddress().getPort() + 100;
        localSocketAddress = new InetSocketAddress(memberLocal.getInetAddress(), port);
        updateAddresses();
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
            SocketAddress sa = (member.localMember()) ? localSocketAddress : new InetSocketAddress(memberImpl.getInetAddress(), memberImpl.getPort() + 100);
            MemberStateImpl memberState = (member.localMember()) ? localState : new MemberStateImpl();
            memberState.setAddress(memberImpl.getAddress());
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
                    ClientHandler clientHandler = qClientHandlers.poll();
                    serverSocket.doAccept(clientHandler.getSocket());
                    clientHandler.start();
                }
            } catch (IOException ignored) {
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
            } catch (Exception ignored) {
                ignored.printStackTrace();
            }
        }
    }

    class UDPSender extends Thread {
        final DatagramSocket socket;
        final DatagramPacket packet = new DatagramPacket(new byte[0], 0);
        final ByteBuffer bbState = ByteBuffer.allocate(1000000);
        final DataOutputStream dos = new DataOutputStream(newOutputStream(bbState));

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
            updateState();
            if (members != null) {
                for (SocketAddress address : members.socketAddresses) {
                    if (!localSocketAddress.equals(address)) {
                        try {
                            bbState.clear();
                            localState.writeData(dos);
                            packet.setData(bbState.array(), 0, bbState.position());
                            packet.setSocketAddress(address);
                            socket.send(packet);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }

        void updateState() {
            factory.createMemberState(localState);
        }
    }

    class LazyDataInputStream extends DataInputStream {
        LazyDataInputStream() {
            super(null);
        }

        void setInputStream(InputStream in) {
            super.in = in;
        }
    }

    class LazyDataOutputStream extends DataOutputStream {
        LazyDataOutputStream() {
            super(null);
        }

        void setOutputStream(OutputStream out) {
            super.out = out;
        }
    }

    class ClientHandler extends Thread {
        final ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
        final DataOutputStream dos = new DataOutputStream(newOutputStream(buffer));
        final ConsoleRequest[] consoleRequests = new ConsoleRequest[10];
        final Socket socket = new Socket();
        final LazyDataInputStream socketIn = new LazyDataInputStream();
        final LazyDataOutputStream socketOut = new LazyDataOutputStream();

        public ClientHandler() {
            register(new GetClusterStateRequest());
            register(new ThreadDumpRequest());
        }

        public void register(ConsoleRequest consoleRequest) {
            consoleRequests[consoleRequest.getType()] = consoleRequest;
        }

        public Socket getSocket() {
            return socket;
        }

        public void run() {
            try {
                socketIn.setInputStream(socket.getInputStream());
                socketOut.setOutputStream(socket.getOutputStream());
                while (running) {
                    int requestType = socketIn.read();
                    ConsoleRequest consoleRequest = consoleRequests[requestType];
                    System.out.println("console.request " + consoleRequest);
                    consoleRequest.readData(socketIn);
                    buffer.clear();
                    boolean isOutOfMemory = factory.node.isOutOfMemory();
                    if (isOutOfMemory) {
                        dos.writeByte(STATE_OUT_OF_MEMORY);
                    } else {
                        dos.writeByte(STATE_ACTIVE);
                        consoleRequest.writeResponse(ManagementConsoleService.this, dos);
                    }
                    socketOut.write(buffer.array(), 0, buffer.position());
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

    public static class GetClusterStateRequest implements ConsoleRequest {
        public int getType() {
            return ManagementConsoleService.REQUEST_TYPE_CLUSTER_STATE;
        }

        public void writeResponse(ManagementConsoleService mcs, DataOutputStream dos) throws Exception {
            mcs.writeState(dos);
        }

        public TimedClusterState readResponse(DataInputStream in) throws IOException {
            TimedClusterState t = new TimedClusterState();
            t.readData(in);
            return t;
        }

        public void writeData(DataOutput out) throws IOException {
        }

        public void readData(DataInput in) throws IOException {
        }
    }

    public Object call(Address address, Callable callable) {
        try {
            Set<Member> members = factory.getCluster().getMembers();
            for (Member member : members) {
                if (address.equals(((MemberImpl) member).getAddress())) {
                    DistributedTask task = new DistributedTask(callable, member);
                    factory.getExecutorService().execute(task);
                    try {
                        return task.get(1, TimeUnit.SECONDS);
                    } catch (Throwable e) {
                        return null;
                    }
                }
            }
        } catch (Throwable e) {
            return null;
        }
        return null;
    }

    public static class GetThreadDump implements Callable<String>, DataSerializable, HazelcastInstanceAware {

        private HazelcastInstance hazelcast;

        public String call() throws Exception {
            return "THREAD DUMP";
        }

        public void writeData(DataOutput out) throws IOException {
        }

        public void readData(DataInput in) throws IOException {
        }

        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.hazelcast = hazelcastInstance;
        }
    }

    public static class ThreadDumpRequest implements ConsoleRequest {
        Address target;

        public ThreadDumpRequest() {
        }

        public ThreadDumpRequest(Address target) {
            this.target = target;
        }

        public int getType() {
            return ManagementConsoleService.REQUEST_TYPE_GET_THREAD_DUMP;
        }

        public void writeResponse(ManagementConsoleService mcs, DataOutputStream dos) throws Exception {
            String threadDump = (String) mcs.call(target, new GetThreadDump());
            dos.writeUTF(threadDump);
        }

        public String readResponse(DataInputStream in) throws IOException {
            return in.readUTF();
        }

        public void writeData(DataOutput out) throws IOException {
            target.writeData(out);
        }

        public void readData(DataInput in) throws IOException {
            target = new Address();
            target.readData(in);
        }
    }

    public static interface ConsoleRequest extends DataSerializable {

        int getType();

        Object readResponse(DataInputStream in) throws IOException;

        void writeResponse(ManagementConsoleService mcs, DataOutputStream dos) throws Exception;
    }

    void writeState(final DataOutputStream dos) throws Exception {
        TimedClusterState timedClusterState = new TimedClusterState();
        for (SocketAddress socketAddress : members.socketAddresses) {
            MemberState memberState = members.getMemberState(socketAddress);
            if (memberState != null) {
                timedClusterState.addMemberState(memberState);
            }
        }
        timedClusterState.setInstanceNames(factory.getLongInstanceNames());
        timedClusterState.writeData(dos);
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
