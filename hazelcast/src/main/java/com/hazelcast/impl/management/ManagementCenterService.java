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
import com.hazelcast.monitor.TimedMemberState;
import com.hazelcast.nio.Address;
import com.hazelcast.partition.Partition;
import com.hazelcast.partition.PartitionService;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.management.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.regex.Pattern;

public class ManagementCenterService implements LifecycleListener {

    private final FactoryImpl factory;
    private volatile boolean running = true;
    private final TaskPoller taskPoller;
    private final StateSender stateSender;
    private final ILogger logger;
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
        logger = factory.node.getLogger(ManagementCenterService.class.getName());
        final ManagementCenterConfig config = factory.node.config.getManagementCenterConfig();
        if (config == null) {
            throw new IllegalStateException("ManagementCenterConfig should not be null!");
        }
        this.instanceFilterMap = new StatsInstanceFilter(factoryImpl.node.getGroupProperties().MC_MAP_EXCLUDES.getString());
        this.instanceFilterQueue = new StatsInstanceFilter(factoryImpl.node.getGroupProperties().MC_QUEUE_EXCLUDES.getString());
        this.instanceFilterTopic = new StatsInstanceFilter(factoryImpl.node.getGroupProperties().MC_TOPIC_EXCLUDES.getString());
        this.instanceFilterAtomicNumber = new StatsInstanceFilter(factoryImpl.node.getGroupProperties().MC_ATOMIC_NUMBER_EXCLUDES.getString());
        this.instanceFilterCountDownLatch = new StatsInstanceFilter(factoryImpl.node.getGroupProperties().MC_COUNT_DOWN_LATCH_EXCLUDES.getString());
        this.instanceFilterSemaphore = new StatsInstanceFilter(factoryImpl.node.getGroupProperties().MC_SEMAPHORE_EXCLUDES.getString());
        maxVisibleInstanceCount = factory.node.groupProperties.MC_MAX_INSTANCE_COUNT.getInteger();
        commandHandler = new ConsoleCommandHandler(factory);

        String tmpWebServerUrl = config != null ? config.getUrl() : null;
        webServerUrl = tmpWebServerUrl != null ?
                (!tmpWebServerUrl.endsWith("/") ? tmpWebServerUrl + '/' : tmpWebServerUrl) : tmpWebServerUrl;
        updateIntervalMs = (config != null && config.getUpdateInterval() > 0) ? config.getUpdateInterval() * 1000 : 3000;

        factory.getLifecycleService().addLifecycleListener(this);
        taskPoller = new TaskPoller();
        stateSender = new StateSender();
        if (config.getUrl() != null) {
            taskPoller.start();
            stateSender.start();
            logger.log(Level.INFO, "Hazelcast Management Center is listening from " + config.getUrl());
        } else {
            logger.log(Level.WARNING, "Hazelcast Management Center Web server url is null!");
        }
        running = true; // volatile-write
    }

    public void shutdown() {
        if (!running) return;
        logger.log(Level.INFO, "Shutting down Hazelcast Management Center");
        running = false;
        try {
            interruptThread(stateSender);
            interruptThread(taskPoller);
        } catch (Throwable ignored) {
        }
    }

    private void interruptThread(Thread t) {
        if (t != null) {
            t.interrupt();
        }
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
                connection.setConnectTimeout(2000);
                connection.setReadTimeout(2000);
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

    private void createMemberState(MemberStateImpl memberState) {
        final Node node = factory.node;
        memberState.setAddress(node.getThisAddress());
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
        if (started.get()) {
            final MemberStateImpl memberState = new MemberStateImpl();
            createMemberState(memberState);
            GroupConfig groupConfig = factory.getConfig().getGroupConfig();
            TimedMemberState timedMemberState = new TimedMemberState();
            timedMemberState.setMaster(factory.node.isMaster());
            if (timedMemberState.getMaster()) {
                timedMemberState.setMemberList(new ArrayList<String>());
                Set<Member> memberSet = factory.getCluster().getMembers();
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
