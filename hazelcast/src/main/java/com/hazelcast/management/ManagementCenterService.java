/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.management;

import com.hazelcast.ascii.rest.HttpCommand;
import com.hazelcast.config.Config;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.config.ManagementCenterConfig;
import com.hazelcast.core.*;
import com.hazelcast.core.LifecycleEvent.LifecycleState;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.OutOfMemoryErrorDispatcher;
import com.hazelcast.logging.ILogger;
import com.hazelcast.management.operation.ManagementCenterConfigOperation;
import com.hazelcast.management.request.*;
import com.hazelcast.map.MapService;
import com.hazelcast.monitor.TimedMemberState;
import com.hazelcast.monitor.impl.*;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.ObjectDataInputStream;
import com.hazelcast.nio.serialization.ObjectDataOutputStream;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.spi.Invocation;
import com.hazelcast.spi.Operation;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.management.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

public class ManagementCenterService implements LifecycleListener, MembershipListener {

    private final HazelcastInstanceImpl instance;
    private final TaskPoller taskPoller;
    private final StateSender stateSender;
    private final ILogger logger;
    private final ConsoleCommandHandler commandHandler;
    private final int maxVisibleInstanceCount;
    private final int updateIntervalMs;
    private final ManagementCenterConfig managementCenterConfig;
    private final SerializationService serializationService;
    private final ManagementCenterIdentifier identifier;
    private AtomicBoolean running = new AtomicBoolean(false);
    private volatile String webServerUrl;
    private volatile boolean urlChanged = false;
    private boolean versionMismatch = false;

    public ManagementCenterService(HazelcastInstanceImpl instance) {
        this.instance = instance;
        logger = this.instance.node.getLogger(ManagementCenterService.class.getName());
        managementCenterConfig = this.instance.node.config.getManagementCenterConfig();
        if (managementCenterConfig == null) {
            throw new IllegalStateException("ManagementCenterConfig should not be null!");
        }
        this.instance.getLifecycleService().addLifecycleListener(this);
        this.instance.getCluster().addMembershipListener(this);
        maxVisibleInstanceCount = this.instance.node.groupProperties.MC_MAX_INSTANCE_COUNT.getInteger();
        commandHandler = new ConsoleCommandHandler(this.instance);
        String tmpWebServerUrl = managementCenterConfig.getUrl();
        webServerUrl = tmpWebServerUrl != null ?
                (!tmpWebServerUrl.endsWith("/") ? tmpWebServerUrl + '/' : tmpWebServerUrl) : null;
        updateIntervalMs = (managementCenterConfig.getUpdateInterval() > 0)
                ? managementCenterConfig.getUpdateInterval() * 1000 : 5000;
        taskPoller = new TaskPoller();
        stateSender = new StateSender();
        serializationService = instance.node.getSerializationService();
        final Address address = instance.node.address;
        identifier = new ManagementCenterIdentifier(instance.node.initializer.getVersion(), instance.getConfig().getGroupConfig().getName(), address.getHost() + ":" + address.getPort());
    }

    public void start() {
        if (running.compareAndSet(false, true)) {
            if (webServerUrl != null) {
                taskPoller.start();
                stateSender.start();
                logger.info("Hazelcast will connect to Management Center on address: " + webServerUrl);
            } else {
                logger.warning("Hazelcast Management Center web-server URL is null!");
            }
        }
    }

    public void shutdown() {
        if (running.compareAndSet(true, false)) {
            logger.info("Shutting down Hazelcast Management Center");
            try {
                interruptThread(stateSender);
                interruptThread(taskPoller);
            } catch (Throwable ignored) {
            }
        }
    }

    public void stateChanged(final LifecycleEvent event) {
        if (event.getState() == LifecycleState.STARTED && managementCenterConfig.isEnabled()) {
            try {
                start();
            } catch (Exception e) {
                logger.severe( "ManagementCenterService could not be started!", e);
            }
        }
    }

    public byte[] changeWebServerUrlOverCluster(String groupName, String groupPass, String newUrl) {
        try {
            GroupConfig groupConfig = instance.getConfig().getGroupConfig();
            if (!(groupConfig.getName().equals(groupName) && groupConfig.getPassword().equals(groupPass)))
                return HttpCommand.RES_403;
            ManagementCenterConfigOperation operation = new ManagementCenterConfigOperation(newUrl);
            sendToAllMembers(operation);
        } catch (Throwable throwable) {
            logger.warning("New web server url cannot be assigned.", throwable);
            return HttpCommand.RES_500;
        }
        return HttpCommand.RES_204;
    }

    public void memberAdded(MembershipEvent membershipEvent) {
        try {
            Member member = membershipEvent.getMember();
            if (member != null && instance.node.isMaster() && urlChanged) {
                ManagementCenterConfigOperation operation = new ManagementCenterConfigOperation(webServerUrl);
                call(((MemberImpl) member).getAddress(), operation);
            }
        } catch (Exception e) {
            logger.warning("Web server url cannot be send to the newly joined member", e);
        }
    }

    public void memberRemoved(MembershipEvent membershipEvent) {
    }

    public void changeWebServerUrl(String newUrl) {
        if (newUrl == null)
            return;
        webServerUrl = newUrl.endsWith("/") ? newUrl : newUrl + "/";
        if (!running.get()) {
            start();
        }
        urlChanged = true;
        logger.info("Web server URL has been changed. " +
                "Hazelcast will connect to Management Center on address: " + webServerUrl);
    }

    private void interruptThread(Thread t) {
        if (t != null) {
            t.interrupt();
        }
    }

//    public List<Edge> detectDeadlock() {
//        Collection<Map<String, MapLockState>> collection =
//                (Collection<Map<String, MapLockState>>) callOnAllMembers(new LockInformationCallable());
//        List<Vertex> graph = new ArrayList<Vertex>();
//        for (Map<String, MapLockState> mapLockStateMap : collection) {
//            for (MapLockState map : mapLockStateMap.values()) {
//                for (Object key : map.getLockOwners().keySet()) {
//                    Vertex owner = new Vertex(map.getLockOwners().get(key));
//                    Vertex requester = new Vertex(map.getLockRequested().get(key));
//                    int index = graph.indexOf(owner);
//                    if (index >= 0) {
//                        owner = graph.get(index);
//                    } else {
//                        graph.add(owner);
//                    }
//                    index = graph.indexOf(requester);
//                    if (index >= 0) {
//                        requester = graph.get(index);
//                    } else {
//                        graph.add(requester);
//                    }
//                    Edge edge = new Edge();
//                    edge.from = requester;
//                    edge.to = owner;
//                    edge.key = key;
//                    edge.mapName = map.getMapName();
//                    edge.globalLock = map.isGlobalLock();
//                    owner.addIncoming(edge);
//                    requester.addOutgoing(edge);
//                }
//            }
//        }
//        List<Edge> list = new ArrayList<Edge>();
//        if (graph != null && graph.size() > 0) {
//            try {
//                graph.get(0).visit(list);
//            } catch (RuntimeException e) {
//            }
//        }
//        return list;
//    }

    public void setVersionMismatch(boolean mismatch) {
        versionMismatch = mismatch;
    }

    private void createMemberState(MemberStateImpl memberState) {
        final Node node = instance.node;
        memberState.setAddress(node.getThisAddress());
        PartitionService partitionService = instance.getPartitionService();
        Set<Partition> partitions = partitionService.getPartitions();
        memberState.clearPartitions();
        for (Partition partition : partitions) {
            if (partition.getOwner() != null && partition.getOwner().localMember()) {
                memberState.addPartition(partition.getPartitionId());
            }
        }
        Collection<DistributedObject> proxyObjects = new ArrayList<DistributedObject>(instance.getDistributedObjects());
        createRuntimeProps(memberState);
        createMemState(memberState, proxyObjects);
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
                                Collection<DistributedObject> distributedObjects) {
        int count = 0;
        final Config config = getHazelcastInstance().getConfig();
        for (DistributedObject distributedObject : distributedObjects) {
            if (count < maxVisibleInstanceCount) {
                if (distributedObject instanceof IMap) {
                    IMap map = (IMap) distributedObject;
                    if (config.getMapConfig(map.getName()).isStatisticsEnabled()) {
                        memberState.putLocalMapStats(map.getName(), (LocalMapStatsImpl) map.getLocalMapStats());
                        count++;
                    }
                } else if (distributedObject instanceof IQueue) {
                    IQueue queue = (IQueue) distributedObject;
                    if (config.getQueueConfig(queue.getName()).isStatisticsEnabled()) {
                        memberState.putLocalQueueStats(queue.getName(), (LocalQueueStatsImpl) queue.getLocalQueueStats());
                        count++;
                    }
                } else if (distributedObject instanceof ITopic) {
                    ITopic topic = (ITopic) distributedObject;
                    if (config.getTopicConfig(topic.getName()).isStatisticsEnabled()) {
                        memberState.putLocalTopicStats(topic.getName(), (LocalTopicStatsImpl) topic.getLocalTopicStats());
                        count++;
                    }
                } else if (distributedObject instanceof MultiMap) {
                    MultiMap multiMap = (MultiMap) distributedObject;
                    if (config.getMultiMapConfig(multiMap.getName()).isStatisticsEnabled()) {
                        memberState.putLocalMultiMapStats(multiMap.getName(), (LocalMultiMapStatsImpl) multiMap.getLocalMultiMapStats());
                        count++;
                    }
                } else if (distributedObject instanceof IExecutorService) {
                    IExecutorService executorService = (IExecutorService) distributedObject;
                    if (config.getExecutorConfig(executorService.getName()).isStatisticsEnabled()) {
                        memberState.putLocalExecutorStats(executorService.getName(), (LocalExecutorStatsImpl) executorService.getLocalExecutorStats());
                        count++;
                    }
                }
            }
        }
    }

    private Set<String> getLongInstanceNames() {
        Set<String> setLongInstanceNames = new HashSet<String>(maxVisibleInstanceCount);
        Collection<DistributedObject> proxyObjects = new ArrayList<DistributedObject>(instance.getDistributedObjects());
        collectInstanceNames(setLongInstanceNames, proxyObjects);
        return setLongInstanceNames;
    }

    //
    private void collectInstanceNames(Set<String> setLongInstanceNames,
                                      Collection<DistributedObject> distributedObjects) {
        int count = 0;
        final Config config = getHazelcastInstance().getConfig();
        for (DistributedObject distributedObject : distributedObjects) {
            if (count < maxVisibleInstanceCount) {
                if (distributedObject instanceof MultiMap) {
                    MultiMap multiMap = (MultiMap) distributedObject;
                    if (config.getMultiMapConfig(multiMap.getName()).isStatisticsEnabled()) {
                        setLongInstanceNames.add("m:" + multiMap.getName());
                        count++;
                    }
                } else if (distributedObject instanceof IMap) {
                    IMap map = (IMap) distributedObject;
                    if (config.getMapConfig(map.getName()).isStatisticsEnabled()) {
                        setLongInstanceNames.add("c:" + map.getName());
                        count++;
                    }
                } else if (distributedObject instanceof IQueue) {
                    IQueue queue = (IQueue) distributedObject;
                    if (config.getQueueConfig(queue.getName()).isStatisticsEnabled()) {
                        setLongInstanceNames.add("q:" + queue.getName());
                        count++;
                    }
                } else if (distributedObject instanceof ITopic) {
                    ITopic topic = (ITopic) distributedObject;
                    if (config.getTopicConfig(topic.getName()).isStatisticsEnabled()) {
                        setLongInstanceNames.add("t:" + topic.getName());
                        count++;
                    }
                } else if (distributedObject instanceof IExecutorService) {
                    IExecutorService executorService = (IExecutorService) distributedObject;
                    if (config.getExecutorConfig(executorService.getName()).isStatisticsEnabled()) {
                        setLongInstanceNames.add("e:" + executorService.getName());
                        count++;
                    }
                }
            }
        }
    }

    public Object call(Address address, Operation operation) {
        Invocation invocation = instance.node.nodeEngine.getOperationService().createInvocationBuilder(MapService.SERVICE_NAME, operation, address).build();
        final Future future = invocation.invoke();
        try {
            return future.get();
        } catch (Throwable t) {
            StringWriter s = new StringWriter();
            t.printStackTrace(new PrintWriter(s));
            return s.toString();
        }
    }

    public void send(Address address, Operation operation) {
        Invocation invocation = instance.node.nodeEngine.getOperationService().createInvocationBuilder(MapService.SERVICE_NAME, operation, address).build();
        invocation.invoke();
    }

    public Collection callOnAddresses(Set<Address> addresses, Operation operation) {
        final ArrayList list = new ArrayList(addresses.size());
        for (Address address : addresses) {
            list.add(call(address, operation));
        }
        return list;
    }

    public Collection callOnAllMembers(Operation operation) {
        Collection<MemberImpl> members = instance.node.clusterService.getMemberList();
        final ArrayList list = new ArrayList(members.size());
        for (MemberImpl member : members) {
            list.add(call(member.getAddress(), operation));

        }
        return list;
    }

    public void sendToAllMembers(Operation operation) {
        Collection<MemberImpl> members = instance.node.clusterService.getMemberList();
        for (MemberImpl member : members) {
            send(member.getAddress(), operation);
        }
    }

    private TimedMemberState getTimedMemberState() {
        if (running.get()) {
            final MemberStateImpl memberState = new MemberStateImpl();
            createMemberState(memberState);
            GroupConfig groupConfig = instance.getConfig().getGroupConfig();
            TimedMemberState timedMemberState = new TimedMemberState();
            timedMemberState.setMaster(instance.node.isMaster());
            if (timedMemberState.getMaster()) {
                timedMemberState.setMemberList(new ArrayList<String>());
                Set<Member> memberSet = instance.getCluster().getMembers();
                for (Member member : memberSet) {
                    MemberImpl memberImpl = (MemberImpl) member;
                    Address address = memberImpl.getAddress();
                    timedMemberState.getMemberList().add(address.getHost() + ":" + address.getPort());
                }
            }
            timedMemberState.setMemberState(memberState);
            timedMemberState.setClusterName(groupConfig.getName());
            timedMemberState.setInstanceNames(getLongInstanceNames());
            return timedMemberState;
        }
        return null;
    }

    public HazelcastInstanceImpl getHazelcastInstance() {
        return instance;
    }

    public ConsoleCommandHandler getCommandHandler() {
        return commandHandler;
    }

    class StateSender extends Thread {
        StateSender() {
            super(instance.getThreadGroup(), instance.node.getThreadNamePrefix("MC.State.Sender"));
        }

        public void run() {
            if (webServerUrl == null) {
                logger.warning("Web server url is null!");
                return;
            }
            try {
                while (running.get()) {
                    if (versionMismatch) {
                        Thread.sleep(1000 * 60);
                        versionMismatch = false;
                    }
                    try {
                        URL url = new URL(webServerUrl + "collector.do");
                        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
                        connection.setDoOutput(true);
                        connection.setRequestMethod("POST");
                        connection.setConnectTimeout(1000);
                        connection.setReadTimeout(1000);
                        final OutputStream outputStream = connection.getOutputStream();
                        identifier.write(outputStream);
                        final ObjectDataOutputStream out = serializationService.createObjectDataOutputStream(outputStream);
                        TimedMemberState ts = getTimedMemberState();
                        ts.writeData(out);
                        out.flush();
                        connection.getInputStream();

                    } catch (Exception e) {
                        logger.finest(e);
                    }
                    Thread.sleep(updateIntervalMs);
                }
            } catch (Throwable throwable) {
                if (throwable instanceof OutOfMemoryError) {
                    OutOfMemoryErrorDispatcher.onOutOfMemory((OutOfMemoryError) throwable);
                }
                logger.finest( "Web Management Center will be closed due to exception.", throwable);
                shutdown();
            }
        }
    }

    class TaskPoller extends Thread {
        final ConsoleRequest[] consoleRequests = new ConsoleRequest[21];

        TaskPoller() {
            super(instance.node.threadGroup, instance.node.getThreadNamePrefix("MC.Task.Poller"));
            register(new RuntimeStateRequest());
            register(new ThreadDumpRequest());
            register(new ExecuteScriptRequest());
            register(new EvictLocalMapRequest());
            register(new ConsoleCommandRequest());
            register(new MapConfigRequest());
            register(new MemberConfigRequest());
            register(new ClusterPropsRequest());
            register(new GetLogsRequest());
            register(new RunGcRequest());
            register(new GetMemberSystemPropertiesRequest());
            register(new GetMapEntryRequest());
            register(new VersionMismatchLogRequest());
            register(new ShutdownMemberRequest());
            register(new GetSystemWarningsRequest());
        }

        public void register(ConsoleRequest consoleRequest) {
            consoleRequests[consoleRequest.getType()] = consoleRequest;
        }

        public void sendResponse(int taskId, ConsoleRequest request) {
            try {
                URL url = new URL(webServerUrl + "putResponse.do");
                HttpURLConnection connection = (HttpURLConnection) url.openConnection();
                connection.setDoOutput(true);
                connection.setRequestMethod("POST");
                connection.setConnectTimeout(2000);
                connection.setReadTimeout(2000);
                final OutputStream outputStream = connection.getOutputStream();
                identifier.write(outputStream);
                final ObjectDataOutputStream out = serializationService.createObjectDataOutputStream(outputStream);
                out.writeInt(taskId);
                out.writeInt(request.getType());
                request.writeResponse(ManagementCenterService.this, out);
                connection.getInputStream();
            } catch (Exception e) {
                logger.finest( e);
            }
        }

        public void run() {
            if (webServerUrl == null) {
                logger.warning("Web server url is null!");
                return;
            }
            try {
                Random rand = new Random();
                Address address = ((MemberImpl) instance.node.getClusterService().getLocalMember()).getAddress();
                GroupConfig groupConfig = instance.getConfig().getGroupConfig();
                while (running.get()) {
                    if (versionMismatch) {
                        Thread.sleep(1000 * 60);
                        versionMismatch = false;
                    }
                    try {
                        URL url = new URL(webServerUrl + "getTask.do?member=" + address.getHost()
                                + ":" + address.getPort() + "&cluster=" + groupConfig.getName());
                        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
                        connection.setRequestProperty("Connection", "keep-alive");
                        InputStream inputStream = connection.getInputStream();
                        ObjectDataInputStream input = serializationService.createObjectDataInputStream(inputStream);
                        final int taskId = input.readInt();
                        final int requestType = input.readInt();
                        if (taskId > 0 && requestType < consoleRequests.length) {
                            final ConsoleRequest request = consoleRequests[requestType];
                            if (request != null) {
                                request.readData(input);
                                sendResponse(taskId, request);
                            }
                        }
                    } catch (Exception e) {
                        logger.finest(e);
                    }
                    Thread.sleep(700 + rand.nextInt(300));
                }
            } catch (Throwable throwable) {
                if (throwable instanceof OutOfMemoryError) {
                    OutOfMemoryErrorDispatcher.onOutOfMemory((OutOfMemoryError) throwable);
                }
                logger.finest( "Problem on management center while polling task.", throwable);
            }
        }
    }
}
