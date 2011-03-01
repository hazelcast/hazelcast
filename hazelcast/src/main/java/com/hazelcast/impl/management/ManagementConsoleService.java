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

package com.hazelcast.impl.management;

import static com.hazelcast.nio.IOUtil.newInputStream;
import static com.hazelcast.nio.IOUtil.newOutputStream;

import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import com.hazelcast.core.DistributedTask;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.core.MultiTask;
import com.hazelcast.impl.FactoryImpl;
import com.hazelcast.impl.MemberImpl;
import com.hazelcast.impl.MemberStateImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.monitor.MemberState;
import com.hazelcast.monitor.TimedClusterState;
import com.hazelcast.nio.Address;

public class ManagementConsoleService implements MembershipListener {

    private final Queue<ClientHandler> qClientHandlers = new LinkedBlockingQueue<ClientHandler>(100);
    private final FactoryImpl factory;
    private volatile boolean running = true;
    private final DatagramSocket datagramSocket;
    private final SocketReadyServerSocket serverSocket;
    private final UDPListener udpListener;
    private final UDPSender udpSender;
    private final TCPListener tcpListener;
    private final List<ClientHandler> lsClientHandlers = new CopyOnWriteArrayList<ClientHandler>();
    private final ILogger logger;
    private static final int DATAGRAM_BUFFER_SIZE = 64 * 1000;
    private final ConcurrentMap<Address, MemberState> memberStates = new ConcurrentHashMap<Address, MemberState>(1000);
    private final ConcurrentMap<Address, SocketAddress> socketAddresses = new ConcurrentHashMap<Address, SocketAddress>(1000);
    private final Set<Address> addresses = new CopyOnWriteArraySet<Address>();
    private volatile MemberStateImpl latestThisMemberState = null;
    private final Address thisAddress;
    private final ConsoleCommandHandler commandHandler ;

    public ManagementConsoleService(FactoryImpl factoryImpl) throws Exception {
        this.factory = factoryImpl;
        thisAddress = ((MemberImpl) factory.getCluster().getLocalMember()).getAddress();
        updateMemberOrder();
        logger = factory.node.getLogger(ManagementConsoleService.class.getName());
        for (int i = 0; i < 100; i++) {
            qClientHandlers.offer(new ClientHandler());
        }
        factory.getCluster().addMembershipListener(this);
        MemberImpl memberLocal = (MemberImpl) factory.getCluster().getLocalMember();
        int port = memberLocal.getInetSocketAddress().getPort() + 100;
        datagramSocket = new DatagramSocket(port);
        serverSocket = new SocketReadyServerSocket(port);
        udpListener = new UDPListener(datagramSocket);
        udpListener.start();
        udpSender = new UDPSender(datagramSocket);
        udpSender.start();
        tcpListener = new TCPListener(serverSocket);
        tcpListener.start();
        commandHandler = new ConsoleCommandHandler(factory);
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
        updateMemberOrder();
    }

    public void memberRemoved(MembershipEvent membershipEvent) {
        Address address = ((MemberImpl) membershipEvent.getMember()).getAddress();
        memberStates.remove(address);
        socketAddresses.remove(address);
        addresses.remove(address);
    }

    void updateMemberOrder() {
        Set<Member> memberSet = factory.getCluster().getMembers();
        for (Member member : memberSet) {
            MemberImpl memberImpl = (MemberImpl) member;
            Address address = memberImpl.getAddress();
            try {
                if (!socketAddresses.containsKey(address)) {
                    SocketAddress socketAddress = new InetSocketAddress(address.getInetAddress(), address.getPort() + 100);
                    socketAddresses.putIfAbsent(address, socketAddress);
                }
                addresses.add(address);
            } catch (UnknownHostException e) {
                e.printStackTrace();
            }
        }
    }

    class TCPListener extends Thread {
        final SocketReadyServerSocket serverSocket;

        TCPListener(SocketReadyServerSocket serverSocket) {
            super("hz.TCP.Listener");
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
        final ByteBuffer bbState = ByteBuffer.allocate(DATAGRAM_BUFFER_SIZE);
        final DatagramPacket packet = new DatagramPacket(bbState.array(), bbState.capacity());
        final DataInputStream dis = new DataInputStream(newInputStream(bbState));

        public UDPListener(DatagramSocket socket) throws SocketException {
            super("hz.UDP.Listener");
            this.socket = socket;
            this.socket.setSoTimeout(1000);
        }

        public void run() {
            try {
                while (running) {
                    try {
                        bbState.clear();
                        socket.receive(packet);
                        bbState.limit(packet.getLength());
                        bbState.position(0);
                        MemberStateImpl memberState = new MemberStateImpl();
                        memberState.readData(dis);
                        memberStates.put(memberState.getAddress(), memberState);
                    } catch (SocketTimeoutException ignored) {
                    }
                }
            } catch (Exception e) {
                logger.log(Level.WARNING, e.getMessage(), e);
            }
        }
    }

    class UDPSender extends Thread {
        final DatagramSocket socket;
        final DatagramPacket packet = new DatagramPacket(new byte[0], 0);
        final ByteBuffer bbState = ByteBuffer.allocate(DATAGRAM_BUFFER_SIZE);
        final DataOutputStream dos = new DataOutputStream(newOutputStream(bbState));

        public UDPSender(DatagramSocket socket) throws SocketException {
            super("hz.UDP.Sender");
            this.socket = socket;
        }

        public void run() {
            try {
                while (running) {
                    sendState();
                    Thread.sleep(5000);
                }
            } catch (Exception e) {
                logger.log(Level.WARNING, e.getMessage(), e);
            }
        }

        void sendState() {
            final MemberState latestState = updateLocalState();
            for (Address address : socketAddresses.keySet()) {
                if (!thisAddress.equals(address)) {
                    final SocketAddress socketAddress = socketAddresses.get(address);
                    if (socketAddress != null) {
                        try {
                            bbState.clear();
                            latestState.writeData(dos);
                            dos.flush();
                            packet.setData(bbState.array(), 0, bbState.position());
                            packet.setSocketAddress(socketAddress);
                            socket.send(packet);
                        } catch (IOException e) {
                            logger.log(Level.WARNING, e.getMessage(), e);
                        }
                    }
                }
            }
        }
    }

    MemberState updateLocalState() {
        latestThisMemberState = factory.createMemberState();
        memberStates.put(latestThisMemberState.getAddress(), latestThisMemberState);
        return latestThisMemberState;
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
        final ConsoleRequest[] consoleRequests = new ConsoleRequest[10];
        final Socket socket = new Socket();
        final LazyDataInputStream socketIn = new LazyDataInputStream();
        final LazyDataOutputStream socketOut = new LazyDataOutputStream();

        public ClientHandler() {
            register(new GetClusterStateRequest());
            register(new ThreadDumpRequest());
            register(new ExecuteScriptRequest());
            register(new EvictLocalMapRequest());
            register(new ConsoleCommandRequest());
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
                    if (requestType == -1) {
                        return;
                    }
                    ConsoleRequest consoleRequest = consoleRequests[requestType];
                    consoleRequest.readData(socketIn);
                    boolean isOutOfMemory = factory.node.isOutOfMemory();
                    if (isOutOfMemory) {
                        socketOut.writeByte(ConsoleRequestConstants.STATE_OUT_OF_MEMORY);
                    } else {
                        socketOut.writeByte(ConsoleRequestConstants.STATE_ACTIVE);
                        consoleRequest.writeResponse(ManagementConsoleService.this, socketOut);
                    }
                }
            } catch (Exception e) {
                logger.log(Level.WARNING, e.getMessage(), e);
            }
        }

        public void shutdown() {
            try {
                socket.close();
            } catch (Throwable ignored) {
            }
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
                        logger.log(Level.FINEST, e.getMessage(), e);
                        return null;
                    }
                }
            }
        } catch (Throwable e) {
            return null;
        }
        return null;
    }

    public Object call(Callable callable) {
        try {
            DistributedTask task = new DistributedTask(callable);
            factory.getExecutorService().execute(task);
            try {
                return task.get(1, TimeUnit.SECONDS);
            } catch (Throwable e) {
                logger.log(Level.FINEST, e.getMessage(), e);
                return null;
            }
        } catch (Throwable e) {
            return null;
        }
    }

    public Object callOnMembers(Set<Address> addresses, Callable callable) {
        Set<Member> members = factory.getCluster().getMembers();
        Set<Member> selectedMembers = new HashSet<Member>(addresses.size());
        for (Member member : members) {
            if (addresses.contains(((MemberImpl) member).getAddress())) {
                selectedMembers.add(member);
            }
        }
        return callOnMembers0(selectedMembers, callable);
    }

    public Collection callOnAllMembers(Callable callable) {
        Set<Member> members = factory.getCluster().getMembers();
        return callOnMembers0(members, callable);
    }

    private Collection callOnMembers0(Set<Member> members, Callable callable) {
        try {
            MultiTask task = new MultiTask(callable, members);
            factory.getExecutorService().execute(task);
            try {
                return task.get(1, TimeUnit.SECONDS);
            } catch (Throwable e) {
                logger.log(Level.FINEST, e.getMessage(), e);
                return null;
            }
        } catch (Throwable e) {
            return null;
        }
    }

    void writeState(final DataOutput dos) throws Exception {
        if (latestThisMemberState == null) {
            updateLocalState();
        }
        TimedClusterState timedClusterState = new TimedClusterState();
        for (Address address : addresses) {
            MemberState memberState = memberStates.get(address);
            if (memberState != null) {
                timedClusterState.addMemberState(memberState);
            }
        }
        timedClusterState.setInstanceNames(factory.getLongInstanceNames());
        timedClusterState.writeData(dos);
    }

    HazelcastInstance getHazelcastInstance() {
        return factory;
    }
    
    ConsoleCommandHandler getCommandHandler() {
    	return commandHandler;
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
