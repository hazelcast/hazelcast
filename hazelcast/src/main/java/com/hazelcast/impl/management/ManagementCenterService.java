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

import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.*;
import com.hazelcast.impl.*;
import com.hazelcast.logging.ILogger;
import com.hazelcast.monitor.MemberState;
import com.hazelcast.monitor.TimedClusterState;
import com.hazelcast.nio.Address;
import com.hazelcast.partition.Partition;
import com.hazelcast.partition.PartitionService;

import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.regex.Pattern;

import static com.hazelcast.nio.IOUtil.newInputStream;
import static com.hazelcast.nio.IOUtil.newOutputStream;

public class ManagementCenterService implements MembershipListener {

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
    private final ConsoleCommandHandler commandHandler;
    private final StatsInstanceFilter instanceFilterMap;
    private final StatsInstanceFilter instanceFilterQueue;
    private final StatsInstanceFilter instanceFilterTopic;

    public ManagementCenterService(FactoryImpl factoryImpl) throws Exception {
        this.factory = factoryImpl;
        this.instanceFilterMap = new StatsInstanceFilter(factoryImpl.node.getGroupProperties().MC_MAP_EXCLUDES.getString());
        this.instanceFilterQueue = new StatsInstanceFilter(factoryImpl.node.getGroupProperties().MC_QUEUE_EXCLUDES.getString());
        this.instanceFilterTopic = new StatsInstanceFilter(factoryImpl.node.getGroupProperties().MC_TOPIC_EXCLUDES.getString());
        thisAddress = ((MemberImpl) factory.getCluster().getLocalMember()).getAddress();
        updateMemberOrder();
        logger = factory.node.getLogger(ManagementCenterService.class.getName());
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
        logger.log(Level.INFO, "Hazelcast Management Center started at port " + port + ".");
    }

    public void shutdown() {
        running = false;
        try {
            datagramSocket.close();
            serverSocket.close();
            for (ClientHandler clientHandler : lsClientHandlers) {
                clientHandler.shutdown();
            }
            udpSender.interrupt();
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
        try {
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
                    logger.log(Level.WARNING, e.getMessage(), e);
                }
            }
        } catch (Throwable e) {
            if (running && factory.node.isActive()) {
                logger.log(Level.WARNING, e.getMessage(), e);
            }
        }
    }

    public boolean login(String groupName, String password) {
        logger.log(Level.INFO, "Management Center Client is trying to login.");
        GroupConfig groupConfig = factory.getConfig().getGroupConfig();
        return groupConfig.getName().equals(groupName) && groupConfig.getPassword().equals(password);
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
            } catch (Throwable e) {
                if (running && factory.node.isActive()) {
                    logger.log(Level.WARNING, e.getMessage(), e);
                }
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
            } catch (Throwable e) {
                if (running && factory.node.isActive()) {
                    logger.log(Level.WARNING, e.getMessage(), e);
                }
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
                            if (running && factory.node.isActive()) {
                                logger.log(Level.WARNING, e.getMessage(), e);
                            }
                        }
                    }
                }
            }
        }
    }

    MemberState updateLocalState() {
        latestThisMemberState = createMemberState();
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

    public MemberStateImpl createMemberState() {
        final MemberStateImpl memberState = new MemberStateImpl();
        createMemberState(memberState);
        return memberState;
    }

    public void createMemberState(MemberStateImpl memberState) {
        final Node node = factory.node;
        memberState.setAddress(((MemberImpl) node.getClusterImpl().getLocalMember()).getAddress());
        memberState.getMemberHealthStats().setOutOfMemory(node.isOutOfMemory());
        memberState.getMemberHealthStats().setActive(node.isActive());
        memberState.getMemberHealthStats().setServiceThreadStats(node.getCpuUtilization().serviceThread);
        memberState.getMemberHealthStats().setOutThreadStats(node.getCpuUtilization().outThread);
        memberState.getMemberHealthStats().setInThreadStats(node.getCpuUtilization().inThread);
        PartitionService partitionService = factory.getPartitionService();
        Set<Partition> partitions = partitionService.getPartitions();
        memberState.clearPartitions();
        for (Partition partition : partitions) {
            if (partition.getOwner() != null && partition.getOwner().localMember()) {
                memberState.addPartition(partition.getPartitionId());
            }
        }
        Collection<HazelcastInstanceAwareInstance> proxyObjects = new ArrayList<HazelcastInstanceAwareInstance>(factory.getProxies());
        createMemState(memberState, proxyObjects.iterator(), Instance.InstanceType.MAP);
        createMemState(memberState, proxyObjects.iterator(), Instance.InstanceType.QUEUE);
        createMemState(memberState, proxyObjects.iterator(), Instance.InstanceType.TOPIC);
    }

    private void createMemState(MemberStateImpl memberState,
                                Iterator<HazelcastInstanceAwareInstance> it,
                                Instance.InstanceType type) {
        int count = 0;
        while (it.hasNext()) {
            HazelcastInstanceAwareInstance proxyObject = it.next();
            if (proxyObject.getInstanceType() == type) {
                if (count < 20) {
                    if (type == Instance.InstanceType.MAP) {
                        MProxy mapProxy = (MProxy) proxyObject;
                        if (instanceFilterMap.visible(mapProxy.getName())) {
                            memberState.putLocalMapStats(mapProxy.getName(), (LocalMapStatsImpl) mapProxy.getLocalMapStats());
                            count++;
                        }
                    } else if (type == Instance.InstanceType.QUEUE) {
                        QProxy qProxy = (QProxy) proxyObject;
                        if (instanceFilterQueue.visible(qProxy.getName())) {
                            memberState.putLocalQueueStats(qProxy.getName(), (LocalQueueStatsImpl) qProxy.getLocalQueueStats());
                            count++;
                        }
                    } else if (type == Instance.InstanceType.TOPIC) {
                        TopicProxy topicProxy = (TopicProxy) proxyObject;
                        if (instanceFilterQueue.visible(topicProxy.getName())) {
                            memberState.putLocalTopicStats(topicProxy.getName(), (LocalTopicStatsImpl) topicProxy.getLocalTopicStats());
                            count++;
                        }
                    }
                }
                it.remove();
            }
        }
    }

    Set<String> getLongInstanceNames() {
        Set<String> setLongInstanceNames = new HashSet<String>(10);
        Collection<HazelcastInstanceAwareInstance> proxyObjects = new ArrayList<HazelcastInstanceAwareInstance>(factory.getProxies());
        collectInstanceNames(setLongInstanceNames, proxyObjects.iterator(), Instance.InstanceType.MAP);
        collectInstanceNames(setLongInstanceNames, proxyObjects.iterator(), Instance.InstanceType.QUEUE);
        collectInstanceNames(setLongInstanceNames, proxyObjects.iterator(), Instance.InstanceType.TOPIC);
        return setLongInstanceNames;
    }

    private void collectInstanceNames(Set<String> setLongInstanceNames,
                                      Iterator<HazelcastInstanceAwareInstance> it,
                                      Instance.InstanceType type) {
        int count = 0;
        while (it.hasNext()) {
            HazelcastInstanceAwareInstance proxyObject = it.next();
            if (proxyObject.getInstanceType() == type) {
                if (count < 20) {
                    if (type == Instance.InstanceType.MAP) {
                        MProxy mapProxy = (MProxy) proxyObject;
                        if (instanceFilterMap.visible(mapProxy.getName())) {
                            setLongInstanceNames.add(mapProxy.getLongName());
                            count++;
                        }
                    } else if (type == Instance.InstanceType.QUEUE) {
                        QProxy qProxy = (QProxy) proxyObject;
                        if (instanceFilterQueue.visible(qProxy.getName())) {
                            setLongInstanceNames.add(qProxy.getLongName());
                            count++;
                        }
                    } else if (type == Instance.InstanceType.TOPIC) {
                        TopicProxy topicProxy = (TopicProxy) proxyObject;
                        if (instanceFilterTopic.visible(topicProxy.getName())) {
                            setLongInstanceNames.add(topicProxy.getLongName());
                            count++;
                        }
                    }
                }
                it.remove();
            }
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
            register(new LoginRequest());
            register(new GetClusterStateRequest());
            register(new ThreadDumpRequest());
            register(new ExecuteScriptRequest());
            register(new EvictLocalMapRequest());
            register(new ConsoleCommandRequest());
            register(new MapConfigRequest());
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
                        consoleRequest.writeResponse(ManagementCenterService.this, socketOut);
                    }
                }
            } catch (Throwable e) {
                if (running && factory.node.isActive()) {
                    logger.log(Level.WARNING, e.getMessage(), e);
                }
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
        Set<Member> members = factory.getCluster().getMembers();
        for (Member member : members) {
            if (address.equals(((MemberImpl) member).getAddress())) {
                DistributedTask task = new DistributedTask(callable, member);
                return executeTaskAndGet(task);
            }
        }
        return null;
    }

    public Object call(Callable callable) {
        DistributedTask task = new DistributedTask(callable);
        return executeTaskAndGet(task);
    }

    public Collection callOnMembers(Set<Address> addresses, Callable callable) {
        Set<Member> allMembers = factory.getCluster().getMembers();
        Set<Member> selectedMembers = new HashSet<Member>(addresses.size());
        for (Member member : allMembers) {
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
        MultiTask task = new MultiTask(callable, members);
        return (Collection) executeTaskAndGet(task);
    }

    private Object executeTaskAndGet(final DistributedTask task) {
        try {
            factory.getExecutorService().execute(task);
            try {
                return task.get(3, TimeUnit.SECONDS);
            } catch (Throwable e) {
                logger.log(Level.FINEST, e.getMessage(), e);
                return null;
            }
        } catch (Throwable e) {
            if (running && factory.node.isActive()) {
                logger.log(Level.WARNING, e.getMessage(), e);
            }
            return null;
        }
    }

    TimedClusterState getState() {
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
        timedClusterState.setInstanceNames(getLongInstanceNames());
        return timedClusterState;
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

    class StatsInstanceFilter {
        final Set<Pattern> setExcludes;
        final Set<String> setIncludeCache;
        final Set<String> setExcludeCache;

        StatsInstanceFilter(String excludes) {
            if (excludes != null) {
                setExcludes = new HashSet<Pattern>();
                setIncludeCache = new HashSet<String>();
                setExcludeCache = new HashSet<String>();
                StringTokenizer st = new StringTokenizer(excludes, ",");
                while (st.hasMoreTokens()) {
                    setExcludes.add(Pattern.compile(st.nextToken().trim()));
                }
            } else {
                setExcludes = null;
                setIncludeCache = null;
                setExcludeCache = null;
            }
        }

        boolean visible(String instanceName) {
            if (setExcludes == null) {
                return true;
            }
            if (setIncludeCache.contains(instanceName)) {
                return true;
            }
            if (setExcludeCache.contains(instanceName)) {
                return false;
            }
            for (Pattern pattern : setExcludes) {
                if (pattern.matcher(instanceName).matches()) {
                    setExcludeCache.add(instanceName);
                    return false;
                }
            }
            setIncludeCache.add(instanceName);
            return true;
        }
    }
}
