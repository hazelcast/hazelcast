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
import com.hazelcast.collection.multimap.ObjectMultiMapProxy;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.config.ManagementCenterConfig;
import com.hazelcast.core.*;
import com.hazelcast.core.LifecycleEvent.LifecycleState;
import com.hazelcast.executor.ExecutorServiceProxy;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.OutOfMemoryErrorDispatcher;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.proxy.MapProxy;
import com.hazelcast.monitor.TimedMemberState;
import com.hazelcast.monitor.impl.*;
import com.hazelcast.nio.Address;
import com.hazelcast.partition.Partition;
import com.hazelcast.queue.proxy.QueueProxy;
import com.hazelcast.topic.proxy.TopicProxy;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.management.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.regex.Pattern;

public class ManagementCenterService implements LifecycleListener, MembershipListener {

    public static final String MANAGEMENT_EXECUTOR = "hz:management";
    private final HazelcastInstanceImpl instance;
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
    private final int updateIntervalMs;
    private final ManagementCenterConfig managementCenterConfig;
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
        this.instanceFilterMap = new StatsInstanceFilter(instance.node.getGroupProperties().MC_MAP_EXCLUDES.getString());
        this.instanceFilterQueue = new StatsInstanceFilter(instance.node.getGroupProperties().MC_QUEUE_EXCLUDES.getString());
        this.instanceFilterTopic = new StatsInstanceFilter(instance.node.getGroupProperties().MC_TOPIC_EXCLUDES.getString());
        this.instanceFilterAtomicNumber = new StatsInstanceFilter(instance.node.getGroupProperties().MC_ATOMIC_NUMBER_EXCLUDES.getString());
        this.instanceFilterCountDownLatch = new StatsInstanceFilter(instance.node.getGroupProperties().MC_COUNT_DOWN_LATCH_EXCLUDES.getString());
        this.instanceFilterSemaphore = new StatsInstanceFilter(instance.node.getGroupProperties().MC_SEMAPHORE_EXCLUDES.getString());
        maxVisibleInstanceCount = this.instance.node.groupProperties.MC_MAX_INSTANCE_COUNT.getInteger();
        commandHandler = new ConsoleCommandHandler(this.instance);
        String tmpWebServerUrl = managementCenterConfig.getUrl();
        webServerUrl = tmpWebServerUrl != null ?
                (!tmpWebServerUrl.endsWith("/") ? tmpWebServerUrl + '/' : tmpWebServerUrl) : tmpWebServerUrl;
        updateIntervalMs = (managementCenterConfig.getUpdateInterval() > 0)
                ? managementCenterConfig.getUpdateInterval() * 1000 : 5000;
        taskPoller = new TaskPoller();
        stateSender = new StateSender();
    }

    public void start() {
        if (running.compareAndSet(false, true)) {
            if (webServerUrl != null) {
                taskPoller.start();
                stateSender.start();
                logger.log(Level.INFO, "Hazelcast will connect to Management Center on address: " + webServerUrl);
            } else {
                logger.log(Level.WARNING, "Hazelcast Management Center web-server URL is null!");
            }
        }
    }

    public void shutdown() {
        if (running.compareAndSet(true, false)) {
            logger.log(Level.INFO, "Shutting down Hazelcast Management Center");
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
                logger.log(Level.SEVERE, "ManagementCenterService could not be started!", e);
            }
        }
    }

    public byte[] changeWebServerUrlOverCluster(String groupName, String groupPass, String newUrl) {
        try {
            GroupConfig groupConfig = instance.getConfig().getGroupConfig();
            if (!(groupConfig.getName().equals(groupName) && groupConfig.getPassword().equals(groupPass)))
                return HttpCommand.RES_403;
            ManagementCenterConfigCallable callable = new ManagementCenterConfigCallable(newUrl);
            callable.setHazelcastInstance(instance);
            Set<Member> members = instance.getCluster().getMembers();
//            MultiTask<Void> task = new MultiTask<Void>(callable, members); //TODO @msk multiTask ???
            ExecutorService executorService = instance.getExecutorService(MANAGEMENT_EXECUTOR);
//            executorService.execute(task);
        } catch (Throwable throwable) {
            logger.log(Level.WARNING, "New web server url cannot be assigned.", throwable);
            return HttpCommand.RES_500;
        }
        return HttpCommand.RES_204;
    }

    public void memberAdded(MembershipEvent membershipEvent) {
        try {
            Member member = membershipEvent.getMember();
            if (member != null && instance.node.isMaster() && urlChanged) {
                ManagementCenterConfigCallable callable = new ManagementCenterConfigCallable(webServerUrl);
//                FutureTask<Void> task = new DistributedTask<Void>(callable, member);//TODO @msk  ???
                ExecutorService executorService = instance.getExecutorService(MANAGEMENT_EXECUTOR);
//                executorService.execute(task);
            }
        } catch (Exception e) {
            logger.log(Level.WARNING, "Web server url cannot be send to the newly joined member", e);
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
        logger.log(Level.INFO, "Web server URL has been changed. " +
                "Hazelcast will connect to Management Center on address: " + webServerUrl);
    }

    private void interruptThread(Thread t) {
        if (t != null) {
            t.interrupt();
        }
    }

//    List<Edge> detectDeadlock() {   //TODO @msk  ???
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
//        memberState.getMemberHealthStats().setOutOfMemory(node.isOutOfMemory());//TODO @msk  ???
        memberState.getMemberHealthStats().setActive(node.isActive());
        PartitionService partitionService = instance.getPartitionService();
        Set<Partition> partitions = partitionService.getPartitions();
        memberState.clearPartitions();
        for (Partition partition : partitions) {
            if (partition.getOwner() != null && partition.getOwner().localMember()) {
                memberState.addPartition(partition.getPartitionId());
            }
        }
        Collection<DistributedObject> proxyObjects = new ArrayList<DistributedObject>(instance.getDistributedObjects());
//        ExecutorManager executorManager = factory.node.executorManager; //TODO @msk  ???
//        ExecutorManager executorManager = instance.node.executorManager;
//        memberState.putInternalThroughputStats(executorManager.getInternalThroughputMap());
//        memberState.putThroughputStats(executorManager.getThroughputMap());

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

    //    private void createMemState(MemberStateImpl memberState,                //TODO @msk  ???
//                                Iterator<DistributedObject> it,
//                                DistributedObject.InstanceType type) {
    private void createMemState(MemberStateImpl memberState,                //TODO @msk  ???
                                Collection<DistributedObject> proxyObjects) {
        int count = 0;
        for (DistributedObject proxyObject : proxyObjects) {
            if (count < maxVisibleInstanceCount) {
                if (proxyObject instanceof MapProxy) {
                    MapProxy mapProxy = (MapProxy) proxyObject;
                    if (instanceFilterMap.visible(mapProxy.getName())) {
                        memberState.putLocalMapStats(mapProxy.getName(), (LocalMapStatsImpl) mapProxy.getLocalMapStats());
                        count++;
                    }
                } else if (proxyObject instanceof QueueProxy) {
                    QueueProxy qProxy = (QueueProxy) proxyObject;
                    if (instanceFilterQueue.visible(qProxy.getName())) {
                        memberState.putLocalQueueStats(qProxy.getName(), (LocalQueueStatsImpl) qProxy.getLocalQueueStats());
                        count++;
                    }
                } else if (proxyObject instanceof TopicProxy) {
                    TopicProxy topicProxy = (TopicProxy) proxyObject;
                    if (instanceFilterTopic.visible(topicProxy.getName())) {
                        memberState.putLocalTopicStats(topicProxy.getName(), (LocalTopicStatsImpl) topicProxy.getLocalTopicStats());
                        count++;
                    }
                } else if (proxyObject instanceof ObjectMultiMapProxy) {
                    ObjectMultiMapProxy multiMapProxy = (ObjectMultiMapProxy) proxyObject;
                    if (instanceFilterTopic.visible(multiMapProxy.getName())) {
                        memberState.putLocalMultiMapStats(multiMapProxy.getName(), (LocalMapStatsImpl) multiMapProxy.getLocalMultiMapStats());
                        count++;
                    }
                } else if (proxyObject instanceof ExecutorServiceProxy) {
                    ExecutorServiceProxy executorServiceProxy = (ExecutorServiceProxy) proxyObject;
//                    if (instanceFilterTopic.visible(multiMapProxy.getName())) {
                    memberState.putLocalExecutorStats(executorServiceProxy.getName(), (LocalExecutorStatsImpl) executorServiceProxy.getLocalExecutorStats());
                    count++;
//                    }
                }
//                     else if (proxyObject instanceof  AtomicLongProxy) {
//                        AtomicLongProxy atomicLongProxy = (AtomicLongProxy) proxyObject;
//                        if (instanceFilterAtomicNumber.visible(atomicLongProxy.getName())) {
//                            memberState.putLocalAtomicNumberStats(atomicLongProxy.getName(), (LocalAtomicLongStatsImpl) atomicLongProxy.getLocalAtomicNumberStats());
//                            count++;
//                        }
//                    }
//                else if (proxyObject instanceof CountDownLatchProxy) {
//                    CountDownLatchProxy cdlProxy = (CountDownLatchProxy) proxyObject;
//                    if (instanceFilterCountDownLatch.visible(cdlProxy.getName())) {
//                        memberState.putLocalCountDownLatchStats(cdlProxy.getName(), (LocalCountDownLatchStatsImpl) cdlProxy.getLocalCountDownLatchStats());
//                        count++;
//                    }
//                    } else if (proxyObject instanceof SemaphoreProxy) {
//                        SemaphoreProxy semaphoreProxy = (SemaphoreProxy) proxyObject;
//                        if (instanceFilterSemaphore.visible(semaphoreProxy.getName())) {
//                            memberState.putLocalSemaphoreStats(semaphoreProxy.getName(), (LocalSemaphoreStatsImpl) semaphoreProxy.getLocalSemaphoreStats());
//                            count++;
//                        }
//                    }
//                }
//                it.remove();
            }
        }
    }

    Object call(Address address, Callable callable) {
        Set<Member> members = instance.getCluster().getMembers();
        for (Member member : members) {
            if (address.equals(((MemberImpl) member).getAddress())) {
//                DistributedTask task = new DistributedTask(callable, member);   //TODO @msk  ???
//                return executeTaskAndGet(task);
            }
        }
        return null;
    }

    Object call(Callable callable) {
//        DistributedTask task = new DistributedTask(callable);     //TODO @msk  ???
//        return executeTaskAndGet(task);
        return null;
    }

    private Set<String> getLongInstanceNames() {
        Set<String> setLongInstanceNames = new HashSet<String>(maxVisibleInstanceCount);
        Collection<DistributedObject> proxyObjects = new ArrayList<DistributedObject>(instance.getDistributedObjects());
        collectInstanceNames(setLongInstanceNames, proxyObjects);
        return setLongInstanceNames;
    }

    //
    private void collectInstanceNames(Set<String> setLongInstanceNames,
                                      Collection<DistributedObject> proxyObjects) {
        int count = 0;
        for (DistributedObject proxyObject : proxyObjects) {
            if (count < maxVisibleInstanceCount) {
                if (proxyObject instanceof ObjectMultiMapProxy) {
                    ObjectMultiMapProxy multiMapProxy = (ObjectMultiMapProxy) proxyObject;
                    if (instanceFilterMap.visible(multiMapProxy.getName())) {
                        setLongInstanceNames.add("m:" + multiMapProxy.getName());
                        count++;
                    }
                } else if (proxyObject instanceof MapProxy) {
                    MapProxy mapProxy = (MapProxy) proxyObject;
                    if (instanceFilterMap.visible(mapProxy.getName())) {
                        setLongInstanceNames.add("c:" + mapProxy.getName());
                        count++;
                    }
                } else if (proxyObject instanceof QueueProxy) {
                    QueueProxy qProxy = (QueueProxy) proxyObject;
                    if (instanceFilterQueue.visible(qProxy.getName())) {
                        setLongInstanceNames.add("q:" + qProxy.getName());
                        count++;
                    }
                } else if (proxyObject instanceof TopicProxy) {
                    TopicProxy topicProxy = (TopicProxy) proxyObject;
                    if (instanceFilterTopic.visible(topicProxy.getName())) {
                        setLongInstanceNames.add("t:" + topicProxy.getName());
                        count++;
                    }
                } else if (proxyObject instanceof ExecutorServiceProxy) {
                    ExecutorServiceProxy executorServiceProxy = (ExecutorServiceProxy) proxyObject;
//                    if (instanceFilterTopic.visible(topicProxy.getName())) {
                    setLongInstanceNames.add("e:" + executorServiceProxy.getName());
                    count++;
//                    }
                }
//                    else if (type.isAtomicNumber()) {
//                        AtomicLongProxy atomicLongProxy = (AtomicLongProxy) proxyObject;
//                        if (instanceFilterAtomicNumber.visible(atomicLongProxy.getName())) {
//                            setLongInstanceNames.add(atomicLongProxy.getLongName());
//                            count++;
//                        }
//                    } else if (type.isCountDownLatch()) {
//                        CountDownLatchProxy cdlProxy = (CountDownLatchProxy) proxyObject;
//                        if (instanceFilterCountDownLatch.visible(cdlProxy.getName())) {
//                            setLongInstanceNames.add(cdlProxy.getLongName());
//                            count++;
//                        }
//                    } else if (type.isSemaphore()) {
//                        SemaphoreProxy semaphoreProxy = (SemaphoreProxy) proxyObject;
//                        if (instanceFilterSemaphore.visible(semaphoreProxy.getName())) {
//                            setLongInstanceNames.add(semaphoreProxy.getLongName());
//                            count++;
//                        }
//                    }
//                    }
//                it.remove();
            }
        }
    }

    Collection callOnMembers(Set<Address> addresses, Callable callable) {
        Set<Member> allMembers = instance.getCluster().getMembers();
        Set<Member> selectedMembers = new HashSet<Member>(addresses.size());
        for (Member member : allMembers) {
            if (addresses.contains(((MemberImpl) member).getAddress())) {
                selectedMembers.add(member);
            }
        }
        return callOnMembers0(selectedMembers, callable);
    }

    Collection callOnAllMembers(Callable callable) {
        Set<Member> members = instance.getCluster().getMembers();
        return callOnMembers0(members, callable);
    }

    private Collection callOnMembers0(Set<Member> members, Callable callable) {
//        MultiTask task = new MultiTask(callable, members);             //TODO @msk  ???
//        return (Collection) executeTaskAndGet(task);
        return null;
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

    HazelcastInstanceImpl getHazelcastInstance() {
        return instance;
    }

//    private Object executeTaskAndGet(final DistributedTask task) {      //TODO @msk  ???
//        try {
//            instance.getExecutorService(MANAGEMENT_EXECUTOR).execute(task);
//            try {
//                return task.get(3, TimeUnit.SECONDS);
//            } catch (Throwable e) {
//                logger.log(Level.FINEST, e.getMessage(), e);
//                return null;
//            }
//        } catch (Throwable e) {
//            if (running.get() && instance.node.isActive()) {
//                logger.log(Level.WARNING, e.getMessage(), e);
//            }
//            return null;
//        }
//    }

    ConsoleCommandHandler getCommandHandler() {
        return commandHandler;
    }

    class StateSender extends Thread {
        StateSender() {
            super(instance.getThreadGroup(), instance.node.getThreadNamePrefix("MC.State.Sender"));
        }

        public void run() {
            if (webServerUrl == null) {
                logger.log(Level.WARNING, "Web server url is null!");
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
                        final DataOutputStream out = new DataOutputStream(connection.getOutputStream());
                        final ObjectDataOutputWrapper wrappedOut = new ObjectDataOutputWrapper(out);
                        TimedMemberState ts = getTimedMemberState();
                        out.writeUTF(instance.node.initializer.getVersion());
//                        factory.node.getThisAddress().writeData(out);    //TODO @msk  ???
                        instance.node.address.writeData(wrappedOut);
                        out.writeUTF(instance.getConfig().getGroupConfig().getName());
//                        ts.writeData(out);//TODO @msk  ???
                        ts.writeData(wrappedOut);
                        out.flush();
                        connection.getInputStream();

                    } catch (Exception e) {
                        logger.log(Level.FINEST, e.getMessage(), e);
                    }
                    Thread.sleep(updateIntervalMs);
                }
            } catch (Throwable throwable) {
                if (throwable instanceof OutOfMemoryError) {
                    OutOfMemoryErrorDispatcher.onOutOfMemory((OutOfMemoryError) throwable);
                }
                logger.log(Level.FINEST, "Web Management Center will be closed due to exception.", throwable);
                shutdown();
            }
        }
    }

    class TaskPoller extends Thread {
        final ConsoleRequest[] consoleRequests = new ConsoleRequest[20];

        TaskPoller() {
            super(instance.node.threadGroup, instance.node.getThreadNamePrefix("MC.Task.Poller"));
            register(new RuntimeStateRequest());
            register(new ThreadDumpRequest());
            register(new ExecuteScriptRequest());
            register(new EvictLocalMapRequest());
            register(new ConsoleCommandRequest());
            register(new MapConfigRequest());
//            register(new DetectDeadlockRequest());
            register(new MemberConfigRequest());
            register(new ClusterPropsRequest());
            register(new SetLogLevelRequest());
            register(new GetLogLevelRequest());
            register(new GetVersionRequest());
            register(new GetLogsRequest());
            register(new RunGcRequest());
            register(new GetMemberSystemPropertiesRequest());
            register(new GetMapEntryRequest());
            register(new VersionMismatchLogRequest());
            register(new ShutdownMemberRequest());
            register(new RestartMemberRequest());
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
                OutputStream outputStream = connection.getOutputStream();
                DataOutputStream output = new DataOutputStream(outputStream);
                output.writeInt(taskId);
                output.writeInt(request.getType());
//                request.writeResponse(ManagementCenterService.this, output);  //TODO @msk  ???
                request.writeResponse(ManagementCenterService.this, new ObjectDataOutputWrapper(output));
                connection.getInputStream();
            } catch (Exception e) {
                logger.log(Level.FINEST, e.getMessage(), e);
            }
        }

        public void run() {
            if (webServerUrl == null) {
                logger.log(Level.WARNING, "Web server url is null!");
                return;
            }
            try {
                Random rand = new Random();
                Address address = ((MemberImpl) instance.node.getClusterService().getLocalMember()).getAddress();
                GroupConfig groupConfig = instance.getConfig().getGroupConfig();
                while (running.get()) {
                    try {
                        URL url = new URL(webServerUrl + "getTask.do?member=" + address.getHost()
                                + ":" + address.getPort() + "&cluster=" + groupConfig.getName());
                        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
                        connection.setRequestProperty("Connection", "keep-alive");
                        InputStream inputStream = connection.getInputStream();
                        DataInputStream input = new DataInputStream(inputStream);
                        final int taskId = input.readInt();
                        final int requestType = input.readInt();
                        if (taskId > 0 && requestType < consoleRequests.length) {
                            final ConsoleRequest request = consoleRequests[requestType];
                            if (request != null) {
//                                request.readData(input); //TODO @msk  ???
                                request.readData(new ObjectDataInputWrapper(input));
                                sendResponse(taskId, request);
                            }
                        }
                    } catch (Exception e) {
                        logger.log(Level.FINEST, e.getMessage(), e);
                    }
                    Thread.sleep(700 + rand.nextInt(300));
                }
            } catch (Throwable throwable) {
                if (throwable instanceof OutOfMemoryError) {
                    OutOfMemoryErrorDispatcher.onOutOfMemory((OutOfMemoryError) throwable);
                }
                logger.log(Level.FINEST, "Problem on management center while polling task.", throwable);
            }
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
