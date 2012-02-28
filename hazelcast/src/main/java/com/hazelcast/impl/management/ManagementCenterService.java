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

package com.hazelcast.impl.management;

import com.hazelcast.config.GroupConfig;
import com.hazelcast.config.ManagementCenterConfig;
import com.hazelcast.core.*;
import com.hazelcast.core.Instance.InstanceType;
import com.hazelcast.impl.*;
import com.hazelcast.impl.management.DetectDeadlockRequest.Edge;
import com.hazelcast.impl.management.DetectDeadlockRequest.Vertex;
import com.hazelcast.impl.management.LockInformationCallable.MapLockState;
import com.hazelcast.impl.monitor.*;
import com.hazelcast.logging.ILogger;
import com.hazelcast.monitor.MemberState;
import com.hazelcast.monitor.TimedClusterState;
import com.hazelcast.monitor.TimedMemberState;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.PipedZipBufferFactory;
import com.hazelcast.nio.PipedZipBufferFactory.DeflatingPipedBuffer;
import com.hazelcast.nio.PipedZipBufferFactory.InflatingPipedBuffer;
import com.hazelcast.partition.Partition;
import com.hazelcast.partition.PartitionService;

import java.io.*;
import java.lang.management.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.regex.Pattern;
import java.util.zip.Deflater;

public class ManagementCenterService implements MembershipListener, LifecycleListener {

    private static final int DATAGRAM_BUFFER_SIZE = 64 * 1024;

    private final Queue<ClientHandler> qClientHandlers = new LinkedBlockingQueue<ClientHandler>(100);
    private final FactoryImpl factory;
    private volatile boolean running = true;
    private DatagramSocket datagramSocket;
    private SocketReadyServerSocket serverSocket;
    private UDPListener udpListener;
    private UDPSender udpSender;
    private TCPListener tcpListener;
    private TaskPoller taskPoller;
    private StateSender stateSender;
    private final List<ClientHandler> lsClientHandlers = new CopyOnWriteArrayList<ClientHandler>();
    private final ILogger logger;
    private final ConcurrentMap<Address, MemberState> memberStates = new ConcurrentHashMap<Address, MemberState>(1000);
    private final ConcurrentMap<Address, SocketAddress> socketAddresses = new ConcurrentHashMap<Address, SocketAddress>(1000);
    private final Set<Address> addresses = new CopyOnWriteArraySet<Address>(); // should be ordered, thread-safe Set
    private volatile MemberStateImpl latestThisMemberState = null;
    private final Address thisAddress;
    private final ConsoleCommandHandler commandHandler;
    private final StatsInstanceFilter instanceFilterMap;
    private final StatsInstanceFilter instanceFilterQueue;
    private final StatsInstanceFilter instanceFilterTopic;
    private final StatsInstanceFilter instanceFilterAtomicNumber;
    private final StatsInstanceFilter instanceFilterCountDownLatch;
    private final StatsInstanceFilter instanceFilterSemaphore;
    private final int maxVisibleInstanceCount;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final String webServerUrl;
    private final int updateIntervalMs;

    @SuppressWarnings("CallToThreadStartDuringObjectConstruction")
    public ManagementCenterService(FactoryImpl factoryImpl) throws Exception {
        this.factory = factoryImpl;
        final ManagementCenterConfig config = factory.node.config.getManagementCenterConfig();
        this.instanceFilterMap = new StatsInstanceFilter(factoryImpl.node.getGroupProperties().MC_MAP_EXCLUDES.getString());
        this.instanceFilterQueue = new StatsInstanceFilter(factoryImpl.node.getGroupProperties().MC_QUEUE_EXCLUDES.getString());
        this.instanceFilterTopic = new StatsInstanceFilter(factoryImpl.node.getGroupProperties().MC_TOPIC_EXCLUDES.getString());
        this.instanceFilterAtomicNumber = new StatsInstanceFilter(factoryImpl.node.getGroupProperties().MC_ATOMIC_NUMBER_EXCLUDES.getString());
        this.instanceFilterCountDownLatch = new StatsInstanceFilter(factoryImpl.node.getGroupProperties().MC_COUNT_DOWN_LATCH_EXCLUDES.getString());
        this.instanceFilterSemaphore = new StatsInstanceFilter(factoryImpl.node.getGroupProperties().MC_SEMAPHORE_EXCLUDES.getString());
        updateMemberOrder();
        logger = factory.node.getLogger(ManagementCenterService.class.getName());
        maxVisibleInstanceCount = factory.node.groupProperties.MC_MAX_INSTANCE_COUNT.getInteger();
        commandHandler = new ConsoleCommandHandler(factory);
        
        String tmpWebServerUrl = config != null ? config.getUrl() : null;
        webServerUrl = tmpWebServerUrl != null ?
                (!tmpWebServerUrl.endsWith("/") ? tmpWebServerUrl + '/' : tmpWebServerUrl) : tmpWebServerUrl;
        updateIntervalMs = (config != null && config.getUpdateInterval() > 0) ? config.getUpdateInterval() * 1000 : 3000;
        
        factory.getCluster().addMembershipListener(this);
        factory.getLifecycleService().addLifecycleListener(this);
        
        final MemberImpl memberLocal = (MemberImpl) factory.getCluster().getLocalMember();
        thisAddress = memberLocal.getAddress();
        if (factory.node.groupProperties.MANCENTER_ENABLED.getBoolean()) {
            int port = calculatePort(thisAddress);
            datagramSocket = new DatagramSocket(port);
            serverSocket = new SocketReadyServerSocket(port, 1000, factory.node.config.isReuseAddress());
            udpListener = new UDPListener(datagramSocket, 1000, factory.node.config.isReuseAddress());
            udpSender = new UDPSender(datagramSocket);
            tcpListener = new TCPListener(serverSocket);
            for (int i = 0; i < 100; i++) {
                qClientHandlers.offer(new ClientHandler(i));
            }
            udpSender.start();
            tcpListener.start();
            udpListener.start();
            logger.log(Level.INFO, "Hazelcast Management Center started at port " + port + ".");
        }
        if (config != null && config.isEnabled()) {
            if (config.getUrl() != null) {
                taskPoller = new TaskPoller();
                stateSender = new StateSender();
                taskPoller.start();
                stateSender.start();
                logger.log(Level.INFO, "Hazelcast Management Center is listening from " + config.getUrl());
            } else {
                logger.log(Level.WARNING, "Hazelcast Management Center Web server url is null!");
            }
        }
        running = true; // volatile-write
    }

    public void shutdown() {
        if (!running) return;
        logger.log(Level.INFO, "Shutting down Hazelcast Management Center");
        running = false;
        try {
            if (datagramSocket != null) {
                datagramSocket.close();
            }
        } catch (Throwable ignored) {
        }
        try {
            if (serverSocket != null) {
                serverSocket.close();
            }
        } catch (Throwable ignored) {
        }
        try {
            interruptThread(udpSender);
            interruptThread(stateSender);
            interruptThread(taskPoller);
            for (ClientHandler clientHandler : lsClientHandlers) {
                clientHandler.shutdown();
            }
            lsClientHandlers.clear();
        } catch (Throwable ignored) {
        }
    }
    
    private void interruptThread(Thread t) {
        if (t != null) {
            t.interrupt();
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

    private void updateMemberOrder() {
        try {
            Set<Member> memberSet = factory.getCluster().getMembers();
            for (Member member : memberSet) {
                MemberImpl memberImpl = (MemberImpl) member;
                Address address = memberImpl.getAddress();
                try {
                    if (!socketAddresses.containsKey(address)) {
                        SocketAddress socketAddress = new InetSocketAddress(address.getInetAddress(), calculatePort(address));
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

    private int calculatePort(final Address address) {
        int port = (address.getPort() - factory.node.config.getPort())
                + factory.node.getGroupProperties().MC_PORT.getInteger();
        return port;
    }

    boolean login(String groupName, String password) {
        logger.log(Level.INFO, "Management Center Client is trying to login.");
        GroupConfig groupConfig = factory.getConfig().getGroupConfig();
        return groupConfig.getName().equals(groupName) && groupConfig.getPassword().equals(password);
    }

    List<Edge> detectDeadlock() {
        Collection<Map<String, MapLockState>> collection =
                (Collection<Map<String, MapLockState>>) callOnAllMembers(new LockInformationCallable());
        List<Vertex> graph = new ArrayList<Vertex>();
        for (Map<String, MapLockState> mapLockStateMap : collection) {
            for (MapLockState map : mapLockStateMap.values()) {
                for (Object key : map.getLockOwners().keySet()) {
                    Vertex owner = new Vertex(map.getLockOwners().get(key));
                    Vertex requester = new Vertex(map.getLockRequested().get(key));
                    int index = graph.indexOf(owner);
                    if (index >= 0) {
                        owner = graph.get(index);
                    } else {
                        graph.add(owner);
                    }
                    index = graph.indexOf(requester);
                    if (index >= 0) {
                        requester = graph.get(index);
                    } else {
                        graph.add(requester);
                    }
                    Edge edge = new Edge();
                    edge.from = requester;
                    edge.to = owner;
                    edge.key = key;
                    edge.mapName = map.getMapName();
                    edge.globalLock = map.isGlobalLock();
                    owner.addIncoming(edge);
                    requester.addOutgoing(edge);
                }
            }
        }
        List<Edge> list = new ArrayList<Edge>();
        if (graph != null && graph.size() > 0) {
            try {
                graph.get(0).visit(list);
            } catch (RuntimeException e) {
            }
        }
        return list;
    }

    class TCPListener extends Thread {
        final SocketReadyServerSocket serverSocket;

        TCPListener(SocketReadyServerSocket serverSocket) {
            super(factory.node.threadGroup, factory.node.getThreadNamePrefix("MC.TCP.Listener"));
            this.serverSocket = serverSocket;
        }

        public void run() {
            try {
                while (running) {
                    final ClientHandler clientHandler = qClientHandlers.poll();
                    if (clientHandler == null) {
                        logger.log(Level.SEVERE, "ClientHandler pool exhausted! Try to connect another node...");
                        break;
                    }
                    try {
                        serverSocket.doAccept(clientHandler.getSocket());
                    } catch (SocketTimeoutException e) {
                        qClientHandlers.offer(clientHandler);
                        continue;
                    }
                    clientHandler.start();
                }
            } catch (Throwable throwable) {
                if (running) {
                    logger.log(Level.FINEST, "ManagementCenter will be closed due to exception.", throwable);
                }
                shutdown();
            }
        }
    }


    class StateSender extends Thread {
        final String host;

        StateSender() {
            super(factory.node.threadGroup, factory.node.getThreadNamePrefix("MC.State.Sender"));
            this.host = webServerUrl;
        }

        public void run() {
            if (host == null) {
                logger.log(Level.WARNING, "Web server url is null!");
                return;
            }
            try {
                while (running) {
                    if (started.get()) {
                        try {
                            URL url = new URL(host + "collector.do");
                            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
                            connection.setDoOutput(true);
                            connection.setRequestMethod("POST");
                            connection.setConnectTimeout(1000);
                            connection.setReadTimeout(1000);
                            final DataOutputStream out = new DataOutputStream(connection.getOutputStream());
                            TimedMemberState ts = getTimedMemberState();
                            ts.writeData(out);
                            out.flush();
                            connection.getInputStream();
                        } catch (Exception e) {
                            logger.log(Level.FINEST, e.getMessage(), e);
                        }
                    }
                    Thread.sleep(updateIntervalMs);
                }
            } catch (Throwable throwable) {
                logger.log(Level.FINEST, "Web Management Center will be closed due to exception.", throwable);
                shutdown();
            }
        }
    }

    class TaskPoller extends Thread {
        final ConsoleRequest[] consoleRequests = new ConsoleRequest[10];
        final String host;

        TaskPoller() {
            super(factory.node.threadGroup, factory.node.getThreadNamePrefix("MC.Task.Poller"));
            this.host = webServerUrl;
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

        public void sendResponse(int taskId, ConsoleRequest request) {
            try {
                URL url = new URL(host + "putResponse.do");
                HttpURLConnection connection = (HttpURLConnection) url.openConnection();
                connection.setDoOutput(true);
                connection.setRequestMethod("POST");
                connection.setConnectTimeout(1000);
                connection.setReadTimeout(1000);
                OutputStream outputStream = connection.getOutputStream();
                DataOutputStream output = new DataOutputStream(outputStream);
                output.writeInt(taskId);
                output.writeInt(request.getType());
                request.writeResponse(ManagementCenterService.this, output);
                connection.getInputStream();
            } catch (Exception e) {
                logger.log(Level.FINEST, e.getMessage(), e);
            }
        }

        public void run() {
            if (host == null) {
                logger.log(Level.WARNING, "Web server url is null!");
                return;
            }
            try {
                Random rand = new Random();
                Address address = ((MemberImpl) factory.node.getClusterImpl().getLocalMember()).getAddress();
                GroupConfig groupConfig = factory.getConfig().getGroupConfig();
                while (running) {
                    try {
                        URL url = new URL(host + "getTask.do?member=" + address.getHost() + ":" + address.getPort() + "&cluster=" + groupConfig.getName());
                        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
                        connection.setRequestProperty("Connection", "keep-alive");
                        InputStream inputStream = connection.getInputStream();
                        DataInputStream input = new DataInputStream(inputStream);

                        int taskId = input.readInt();
                        if (taskId > 0) {
                            int requestType = input.readInt();
                            ConsoleRequest request = consoleRequests[requestType];
                            request.readData(input);
                            sendResponse(taskId, request);
                        }
                    } catch (Exception e) {
                        logger.log(Level.FINEST, e.getMessage(), e);
                    }
                    Thread.sleep(700 + rand.nextInt(300));
                }
            } catch (Throwable throwable) {
                logger.log(Level.FINEST, "Web Management Center will be closed due to exception.", throwable);
            }
        }
    }


    class UDPListener extends Thread {
        final DatagramSocket socket;
        final InflatingPipedBuffer buffer = PipedZipBufferFactory.createInflatingBuffer(DATAGRAM_BUFFER_SIZE);
        final DatagramPacket packet = new DatagramPacket(buffer.getInputBuffer().array(), DATAGRAM_BUFFER_SIZE);

        public UDPListener(DatagramSocket socket, int timeout, boolean reuseAddress) throws SocketException {
            super(factory.node.threadGroup, factory.node.getThreadNamePrefix("MC.UDP.Listener"));
            this.socket = socket;
            this.socket.setSoTimeout(timeout);
            this.socket.setReuseAddress(reuseAddress);
        }

        public void run() {
            try {
                while (running) {
                    try {
                        buffer.reset();
                        socket.receive(packet);
                        buffer.inflate(packet.getLength());
                        MemberStateImpl memberState = new MemberStateImpl();
                        memberState.readData(buffer.getDataInput());
                        memberStates.put(memberState.getAddress(), memberState);
                    } catch (SocketTimeoutException ignored) {
                    }
                }
            } catch (Throwable e) {
                if (running && factory.node.isActive()) {
                    logger.log(Level.WARNING, e.getMessage(), e);
                }
            } finally {
                buffer.destroy();
                packet.setData(new byte[0]);
            }
        }
    }

    private class UDPSender extends Thread {
        final DatagramSocket socket;
        final DatagramPacket packet = new DatagramPacket(new byte[0], 0);
        final DeflatingPipedBuffer buffer = PipedZipBufferFactory.createDeflatingBuffer(DATAGRAM_BUFFER_SIZE, Deflater.BEST_SPEED);

        public UDPSender(DatagramSocket socket) throws SocketException {
            super(factory.node.threadGroup, factory.node.getThreadNamePrefix("MC.UDP.Sender"));
            this.socket = socket;
        }

        public void run() {
            try {
                while (running) {
                    if (started.get()) {
                        updateLocalState();
                        sendState();
                    }
                    //noinspection BusyWait
                    Thread.sleep(5000);
                }
            } catch (Throwable e) {
                if (running && factory.node.isActive()) {
                    logger.log(Level.WARNING, e.getMessage(), e);
                }
            } finally {
                buffer.destroy();
                packet.setData(new byte[0]);
            }
        }

        private void sendState() {
            boolean preparedStateData = false;
            int compressedCount = 0;
            for (Address address : socketAddresses.keySet()) {
                if (!thisAddress.equals(address)) {
                    final SocketAddress socketAddress = socketAddresses.get(address);
                    if (socketAddress != null) {
                        try {
                            if (!preparedStateData) {
                                compressedCount = prepareStateData();
                                preparedStateData = true;
                            }
                            packet.setData(buffer.getOutputBuffer().array(), 0, compressedCount);
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

        private int prepareStateData() throws IOException {
            final MemberState latestState = latestThisMemberState;
            buffer.reset();
            if (latestState != null) {
                latestState.writeData(buffer.getDataOutput());
                return buffer.deflate();
            }
            return 0;
        }
    }

    private void updateLocalState() {
        if (started.get()) {
            latestThisMemberState = createMemberState();
            memberStates.put(latestThisMemberState.getAddress(), latestThisMemberState);
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

    private MemberStateImpl createMemberState() {
        if (started.get()) {
            final MemberStateImpl memberState = new MemberStateImpl();
            createMemberState(memberState);
            return memberState;
        }
        return null;
    }

    private void createMemberState(MemberStateImpl memberState) {
        final Node node = factory.node;
        memberState.setAddress(thisAddress);
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
        createMemState(memberState, proxyObjects.iterator(), InstanceType.MAP);
        createMemState(memberState, proxyObjects.iterator(), InstanceType.QUEUE);
        createMemState(memberState, proxyObjects.iterator(), InstanceType.TOPIC);
        createRuntimeProps(memberState);
        // uncomment when client changes are made
        //createMemState(memberState, proxyObjects.iterator(), InstanceType.ATOMIC_LONG);
        //createMemState(memberState, proxyObjects.iterator(), InstanceType.COUNT_DOWN_LATCH);
        //createMemState(memberState, proxyObjects.iterator(), InstanceType.SEMAPHORE);
    }

    private void createRuntimeProps(MemberStateImpl memberState) {

        Runtime runtime = Runtime.getRuntime();
        ThreadMXBean threadMxBean = ManagementFactory.getThreadMXBean();
        RuntimeMXBean runtimeMxBean = ManagementFactory.getRuntimeMXBean();
        ClassLoadingMXBean clMxBean = ManagementFactory.getClassLoadingMXBean();
        MemoryMXBean memoryMxBean = ManagementFactory.getMemoryMXBean();
        MemoryUsage heapMemory = memoryMxBean.getHeapMemoryUsage();
        MemoryUsage nonHeapMemory = memoryMxBean.getNonHeapMemoryUsage();

        Map<String, Long> map = new HashMap<String, Long>();
        map.put("runtime.availableProcessors", Integer.valueOf(runtime.availableProcessors()).longValue());
        map.put("date.startTime", runtimeMxBean.getStartTime());
        map.put("seconds.upTime", runtimeMxBean.getUptime());

        map.put("memory.maxMemory", runtime.maxMemory());
        map.put("memory.freeMemory", runtime.freeMemory());
        map.put("memory.totalMemory", runtime.totalMemory());
        map.put("memory.heapMemoryMax", heapMemory.getMax());
        map.put("memory.heapMemoryUsed", heapMemory.getUsed());
        map.put("memory.nonHeapMemoryMax", nonHeapMemory.getMax());
        map.put("memory.nonHeapMemoryUsed", nonHeapMemory.getUsed());
        map.put("runtime.totalLoadedClassCount", clMxBean.getTotalLoadedClassCount());
        map.put("runtime.loadedClassCount", Integer.valueOf(clMxBean.getLoadedClassCount()).longValue());
        map.put("runtime.unloadedClassCount", clMxBean.getUnloadedClassCount());
        map.put("runtime.totalStartedThreadCount", threadMxBean.getTotalStartedThreadCount());
        map.put("runtime.threadCount", Integer.valueOf(threadMxBean.getThreadCount()).longValue());
        map.put("runtime.peakThreadCount", Integer.valueOf(threadMxBean.getPeakThreadCount()).longValue());
        map.put("runtime.daemonThreadCount", Integer.valueOf(threadMxBean.getDaemonThreadCount()).longValue());
        memberState.setRuntimeProps(map);

    }

    private void createMemState(MemberStateImpl memberState,
                                Iterator<HazelcastInstanceAwareInstance> it,
                                Instance.InstanceType type) {
        int count = 0;
        while (it.hasNext()) {
            HazelcastInstanceAwareInstance proxyObject = it.next();
            if (proxyObject.getInstanceType() == type) {
                if (count < maxVisibleInstanceCount) {
                    if (type.isMap()) {
                        MProxy mapProxy = (MProxy) proxyObject;
                        if (instanceFilterMap.visible(mapProxy.getName())) {
                            memberState.putLocalMapStats(mapProxy.getName(), (LocalMapStatsImpl) mapProxy.getLocalMapStats());
                            count++;
                        }
                    } else if (type.isQueue()) {
                        QProxy qProxy = (QProxy) proxyObject;
                        if (instanceFilterQueue.visible(qProxy.getName())) {
                            memberState.putLocalQueueStats(qProxy.getName(), (LocalQueueStatsImpl) qProxy.getLocalQueueStats());
                            count++;
                        }
                    } else if (type.isTopic()) {
                        TopicProxy topicProxy = (TopicProxy) proxyObject;
                        if (instanceFilterTopic.visible(topicProxy.getName())) {
                            memberState.putLocalTopicStats(topicProxy.getName(), (LocalTopicStatsImpl) topicProxy.getLocalTopicStats());
                            count++;
                        }
                    } else if (type.isAtomicNumber()) {
                        AtomicNumberProxy atomicLongProxy = (AtomicNumberProxy) proxyObject;
                        if (instanceFilterAtomicNumber.visible(atomicLongProxy.getName())) {
                            memberState.putLocalAtomicNumberStats(atomicLongProxy.getName(), (LocalAtomicNumberStatsImpl) atomicLongProxy.getLocalAtomicNumberStats());
                            count++;
                        }
                    } else if (type.isCountDownLatch()) {
                        CountDownLatchProxy cdlProxy = (CountDownLatchProxy) proxyObject;
                        if (instanceFilterCountDownLatch.visible(cdlProxy.getName())) {
                            memberState.putLocalCountDownLatchStats(cdlProxy.getName(), (LocalCountDownLatchStatsImpl) cdlProxy.getLocalCountDownLatchStats());
                            count++;
                        }
                    } else if (type.isSemaphore()) {
                        SemaphoreProxy semaphoreProxy = (SemaphoreProxy) proxyObject;
                        if (instanceFilterSemaphore.visible(semaphoreProxy.getName())) {
                            memberState.putLocalSemaphoreStats(semaphoreProxy.getName(), (LocalSemaphoreStatsImpl) semaphoreProxy.getLocalSemaphoreStats());
                            count++;
                        }
                    }
                }
                it.remove();
            }
        }
    }

    private Set<String> getLongInstanceNames() {
        Set<String> setLongInstanceNames = new HashSet<String>(maxVisibleInstanceCount);
        Collection<HazelcastInstanceAwareInstance> proxyObjects = new ArrayList<HazelcastInstanceAwareInstance>(factory.getProxies());
        collectInstanceNames(setLongInstanceNames, proxyObjects.iterator(), InstanceType.MAP);
        collectInstanceNames(setLongInstanceNames, proxyObjects.iterator(), InstanceType.QUEUE);
        collectInstanceNames(setLongInstanceNames, proxyObjects.iterator(), InstanceType.TOPIC);
        // uncomment when client changes are made
        // collectInstanceNames(setLongInstanceNames, proxyObjects.iterator(), InstanceType.ATOMIC_NUMBER);
        // collectInstanceNames(setLongInstanceNames, proxyObjects.iterator(), InstanceType.COUNT_DOWN_LATCH);
        // collectInstanceNames(setLongInstanceNames, proxyObjects.iterator(), InstanceType.SEMAPHORE);
        return setLongInstanceNames;
    }

    private void collectInstanceNames(Set<String> setLongInstanceNames,
                                      Iterator<HazelcastInstanceAwareInstance> it,
                                      Instance.InstanceType type) {
        int count = 0;
        while (it.hasNext()) {
            HazelcastInstanceAwareInstance proxyObject = it.next();
            if (proxyObject.getInstanceType() == type) {
                if (count < maxVisibleInstanceCount) {
                    if (type.isMap()) {
                        MProxy mapProxy = (MProxy) proxyObject;
                        if (instanceFilterMap.visible(mapProxy.getName())) {
                            setLongInstanceNames.add(mapProxy.getLongName());
                            count++;
                        }
                    } else if (type.isQueue()) {
                        QProxy qProxy = (QProxy) proxyObject;
                        if (instanceFilterQueue.visible(qProxy.getName())) {
                            setLongInstanceNames.add(qProxy.getLongName());
                            count++;
                        }
                    } else if (type.isTopic()) {
                        TopicProxy topicProxy = (TopicProxy) proxyObject;
                        if (instanceFilterTopic.visible(topicProxy.getName())) {
                            setLongInstanceNames.add(topicProxy.getLongName());
                            count++;
                        }
                    } else if (type.isAtomicNumber()) {
                        AtomicNumberProxy atomicLongProxy = (AtomicNumberProxy) proxyObject;
                        if (instanceFilterAtomicNumber.visible(atomicLongProxy.getName())) {
                            setLongInstanceNames.add(atomicLongProxy.getLongName());
                            count++;
                        }
                    } else if (type.isCountDownLatch()) {
                        CountDownLatchProxy cdlProxy = (CountDownLatchProxy) proxyObject;
                        if (instanceFilterCountDownLatch.visible(cdlProxy.getName())) {
                            setLongInstanceNames.add(cdlProxy.getLongName());
                            count++;
                        }
                    } else if (type.isSemaphore()) {
                        SemaphoreProxy semaphoreProxy = (SemaphoreProxy) proxyObject;
                        if (instanceFilterSemaphore.visible(semaphoreProxy.getName())) {
                            setLongInstanceNames.add(semaphoreProxy.getLongName());
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

        public ClientHandler(int id) {
            super(factory.node.threadGroup, factory.node.getThreadPoolNamePrefix("MC.Client.Handler") + id);
            register(new LoginRequest());
            register(new GetClusterStateRequest());
            register(new ThreadDumpRequest());
            register(new ExecuteScriptRequest());
            register(new EvictLocalMapRequest());
            register(new ConsoleCommandRequest());
            register(new MapConfigRequest());
            register(new DetectDeadlockRequest());
        }

        private void register(ConsoleRequest consoleRequest) {
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
                        logger.log(Level.WARNING, "Management Center Client connection ["
                                + socket.getInetAddress() + "] is closed!");
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
            } finally {
                shutdown();
            }
        }

        private void shutdown() {
            try {
                socket.close();
            } catch (Throwable ignored) {
            }
        }
    }

    Object call(Address address, Callable callable) {
        Set<Member> members = factory.getCluster().getMembers();
        for (Member member : members) {
            if (address.equals(((MemberImpl) member).getAddress())) {
                DistributedTask task = new DistributedTask(callable, member);
                return executeTaskAndGet(task);
            }
        }
        return null;
    }

    Object call(Callable callable) {
        DistributedTask task = new DistributedTask(callable);
        return executeTaskAndGet(task);
    }

    Collection callOnMembers(Set<Address> addresses, Callable callable) {
        Set<Member> allMembers = factory.getCluster().getMembers();
        Set<Member> selectedMembers = new HashSet<Member>(addresses.size());
        for (Member member : allMembers) {
            if (addresses.contains(((MemberImpl) member).getAddress())) {
                selectedMembers.add(member);
            }
        }
        return callOnMembers0(selectedMembers, callable);
    }

    Collection callOnAllMembers(Callable callable) {
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

    public TimedMemberState getTimedMemberState() {
        if (latestThisMemberState == null) {
            updateLocalState();
        }
        GroupConfig groupConfig = factory.getConfig().getGroupConfig();
        TimedMemberState timedMemberState = new TimedMemberState();
        timedMemberState.setMaster(factory.node.isMaster());


        if (timedMemberState.getMaster()) {
            timedMemberState.setMemberList(new ArrayList<String>());
            for (Address addr : addresses) {
                timedMemberState.getMemberList().add(addr.getHost() + ":" + addr.getPort());
            }
        }
        timedMemberState.setMemberState(latestThisMemberState);
        timedMemberState.setClusterName(groupConfig.getName());
        timedMemberState.setInstanceNames(getLongInstanceNames());
        return timedMemberState;
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

    public void stateChanged(final LifecycleEvent event) {
        started.set(event.getState() == LifecycleEvent.LifecycleState.STARTED);
        logger.log(Level.FINEST, "Hazelcast Management Center enabled: " + started.get());
    }

    public static class SocketReadyServerSocket extends ServerSocket {

        public SocketReadyServerSocket(int port, int timeout, boolean reuseAddress) throws IOException {
            super(port);
            setSoTimeout(timeout);
            setReuseAddress(reuseAddress);
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
