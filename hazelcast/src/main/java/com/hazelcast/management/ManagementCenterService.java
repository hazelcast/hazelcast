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


import java.io.*;
import java.lang.management.*;
import java.lang.reflect.Method;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

public class ManagementCenterService implements LifecycleListener, MembershipListener {

    public final static AtomicBoolean DISPLAYED_HOSTED_MANAGEMENT_CENTER_INFO =  new AtomicBoolean(false);

    public static final String HOSTED_MANCENTER_URL = "http://localhost:8085/mancenter";

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
    private final String projectId;
    private final String securityToken;

    public ManagementCenterService(HazelcastInstanceImpl instance) {
        this.instance = instance;
        logger = this.instance.node.getLogger(ManagementCenterService.class.getName());
        managementCenterConfig = this.instance.node.config.getManagementCenterConfig();
        if (managementCenterConfig == null) {
            throw new IllegalStateException("ManagementCenterConfig should not be null!");
        }
        this.securityToken = managementCenterConfig.getSecurityToken();

        String projectId = managementCenterConfig.getProjectId();

        String url = managementCenterConfig.getUrl();

        if(managementCenterConfig.isEnabled() && url==null){

            //if the url is not set, but the management center is enabled, we are going to point him to the hosted management solution.
            //if the url is set, he is running his own management center instance and we are not going to bother him with the
            //hosted management solution.

            if(managementCenterConfig.getSecurityToken() == null){

                //so the user has not provided a security token, so need to tell him that he can create one at
                //our registration page.

                //we only want to display the page for hosted management registration once. We don't want to pollute
                //the logfile.
                if(DISPLAYED_HOSTED_MANAGEMENT_CENTER_INFO.compareAndSet(false,true)){
                    logger.info("======================================================");
                    logger.info("Manage your Hazelcast cluster with the Management Center SaaS Application");
                    logger.info(HOSTED_MANCENTER_URL+"/register.jsp");
                    logger.info("======================================================");
                }
            }else{
                url = HOSTED_MANCENTER_URL;
                //the user has provided a security token.

                if (projectId == null) {
                    //the user has not provided a projectid, so lets generate one for him.
                    IAtomicReference<String> clusterIdAtomicLong = instance.getAtomicReference("___projectIdGenerator");
                    String id = clusterIdAtomicLong.get();
                    if (id == null) {
                        id = "" + Math.abs(new Random().nextLong());
                        if (!clusterIdAtomicLong.compareAndSet(null, id)) {
                            id = clusterIdAtomicLong.get();
                        }
                    }
                    projectId = "" + id;
                }

                logger.info("======================================================");
                logger.info("You can access your Hazelcast instance at:");
                logger.info(url + "/start.do?projectid=" + projectId);
                logger.info("======================================================");
            }
        }
        this.projectId = projectId;

        this.instance.getLifecycleService().addLifecycleListener(this);
        this.instance.getCluster().addMembershipListener(this);
        maxVisibleInstanceCount = this.instance.node.groupProperties.MC_MAX_INSTANCE_COUNT.getInteger();
        commandHandler = new ConsoleCommandHandler(this.instance);

        webServerUrl = url != null ?
                (!url.endsWith("/") ? url + '/' : url) : null;
        updateIntervalMs = (managementCenterConfig.getUpdateInterval() > 0)
                ? managementCenterConfig.getUpdateInterval() * 1000 : 5000;
        taskPoller = new TaskPoller();
        stateSender = new StateSender();
        serializationService = instance.node.getSerializationService();
        final Address address = instance.node.address;
        identifier = new ManagementCenterIdentifier(instance.node.getBuildInfo().getVersion(),
                instance.getConfig().getGroupConfig().getName(), address.getHost() + ":" + address.getPort());
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

            final Collection<MemberImpl> memberList = instance.node.clusterService.getMemberList();
            for (MemberImpl member : memberList) {
                send(member.getAddress(), new ManagementCenterConfigOperation(newUrl));
            }
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
                callOnMember(member, operation);
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

        OperatingSystemMXBean osMxBean = ManagementFactory.getOperatingSystemMXBean();
        map.put("osMemory.freePhysicalMemory", get(osMxBean,"getFreePhysicalMemorySize",0L));
        map.put("osMemory.committedVirtualMemory", get(osMxBean,"getCommittedVirtualMemorySize",0L));
        map.put("osMemory.totalPhysicalMemory", get(osMxBean,"getTotalPhysicalMemorySize",0L));

        map.put("osSwap.freeSwapSpace", get(osMxBean,"getFreeSwapSpaceSize",0L));
        map.put("osSwap.totalSwapSpace", get(osMxBean,"getTotalSwapSpaceSize",0L));
        map.put("os.maxFileDescriptorCount", get(osMxBean,"getMaxFileDescriptorCount",0L));
        map.put("os.openFileDescriptorCount", get(osMxBean,"getOpenFileDescriptorCount",0L))
        ;
        map.put("os.processCpuLoad", get(osMxBean, "getProcessCpuLoad", -1L));
        map.put("os.systemLoadAverage", get(osMxBean, "getSystemLoadAverage", -1L));
        map.put("os.systemCpuLoad", get(osMxBean, "getSystemCpuLoad", -1L));
        map.put("os.processCpuTime", get(osMxBean,"getProcessCpuTime",0L));

        map.put("os.availableProcessors", get(osMxBean,"getAvailableProcessors",0L));

        memberState.setRuntimeProps(map);
    }

    private static Long get(OperatingSystemMXBean mbean, String methodName, Long defaultValue){
        try {
            Method method = mbean.getClass().getMethod(methodName);
            method.setAccessible(true);

            Object value =  method.invoke(mbean);
            if(value == null){
                return defaultValue;
            }

            if(value instanceof Integer){
                return (long) (Integer) value;
            }

            if(value instanceof Double){
               double v = (Double)value;
               return Math.round(v * 100);
            }

            if(value instanceof Long){
                return (Long)value;
            }

            return defaultValue;
        } catch (Exception e) {
            return defaultValue;
        }
    }

    private void createMemState(MemberStateImpl memberState,
                                Collection<DistributedObject> distributedObjects) {
        int count = 0;
        final Config config = getHazelcastInstance().getConfig();
        for (DistributedObject distributedObject : distributedObjects) {
            if (count < maxVisibleInstanceCount) {
                if (distributedObject instanceof IMap) {
                    IMap map = (IMap) distributedObject;
                    if (config.findMapConfig(map.getName()).isStatisticsEnabled()) {
                        memberState.putLocalMapStats(map.getName(), (LocalMapStatsImpl) map.getLocalMapStats());
                        count++;
                    }
                } else if (distributedObject instanceof IQueue) {
                    IQueue queue = (IQueue) distributedObject;
                    if (config.findQueueConfig(queue.getName()).isStatisticsEnabled()) {
                        memberState.putLocalQueueStats(queue.getName(), (LocalQueueStatsImpl) queue.getLocalQueueStats());
                        count++;
                    }
                } else if (distributedObject instanceof ITopic) {
                    ITopic topic = (ITopic) distributedObject;
                    if (config.findTopicConfig(topic.getName()).isStatisticsEnabled()) {
                        memberState.putLocalTopicStats(topic.getName(), (LocalTopicStatsImpl) topic.getLocalTopicStats());
                        count++;
                    }
                } else if (distributedObject instanceof MultiMap) {
                    MultiMap multiMap = (MultiMap) distributedObject;
                    if (config.findMultiMapConfig(multiMap.getName()).isStatisticsEnabled()) {
                        memberState.putLocalMultiMapStats(multiMap.getName(), (LocalMultiMapStatsImpl) multiMap.getLocalMultiMapStats());
                        count++;
                    }
                } else if (distributedObject instanceof IExecutorService) {
                    IExecutorService executorService = (IExecutorService) distributedObject;
                    if (config.findExecutorConfig(executorService.getName()).isStatisticsEnabled()) {
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
                    if (config.findMultiMapConfig(multiMap.getName()).isStatisticsEnabled()) {
                        setLongInstanceNames.add("m:" + multiMap.getName());
                        count++;
                    }
                } else if (distributedObject instanceof IMap) {
                    IMap map = (IMap) distributedObject;
                    if (config.findMapConfig(map.getName()).isStatisticsEnabled()) {
                        setLongInstanceNames.add("c:" + map.getName());
                        count++;
                    }
                } else if (distributedObject instanceof IQueue) {
                    IQueue queue = (IQueue) distributedObject;
                    if (config.findQueueConfig(queue.getName()).isStatisticsEnabled()) {
                        setLongInstanceNames.add("q:" + queue.getName());
                        count++;
                    }
                } else if (distributedObject instanceof ITopic) {
                    ITopic topic = (ITopic) distributedObject;
                    if (config.findTopicConfig(topic.getName()).isStatisticsEnabled()) {
                        setLongInstanceNames.add("t:" + topic.getName());
                        count++;
                    }
                } else if (distributedObject instanceof IExecutorService) {
                    IExecutorService executorService = (IExecutorService) distributedObject;
                    if (config.findExecutorConfig(executorService.getName()).isStatisticsEnabled()) {
                        setLongInstanceNames.add("e:" + executorService.getName());
                        count++;
                    }
                }
            }
        }
    }

    public Object callOnAddress(Address address, Operation operation) {
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

    public Object callOnMember(Member member, Operation operation) {
        Invocation invocation = instance.node.nodeEngine.getOperationService().createInvocationBuilder(MapService.SERVICE_NAME, operation, ((MemberImpl) member).getAddress()).build();
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

    TimedMemberState getTimedMemberState() {
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
                        URL url = createCollectorUrl();
//                        System.out.println(url);
                        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
                        connection.setDoOutput(true);
                        connection.setRequestMethod("POST");
                        connection.setConnectTimeout(1000);
                        connection.setReadTimeout(1000);
                        final OutputStream outputStream = connection.getOutputStream();
                        DataOutputStream dataOutput = new DataOutputStream(outputStream);
                        identifier.write(dataOutput);

                        final ByteArrayOutputStream compressByteArray = getStateAsCompressedArray();

                        dataOutput.writeInt(compressByteArray.size());
                        dataOutput.write(compressByteArray.toByteArray());
                        dataOutput.flush();
                        connection.getInputStream();
                    } catch (Exception e) {
                        logger.warning(e);
                    }
                    Thread.sleep(updateIntervalMs);
                }
            } catch (Throwable throwable) {
                if (throwable instanceof OutOfMemoryError) {
                    OutOfMemoryErrorDispatcher.onOutOfMemory((OutOfMemoryError) throwable);
                }
                logger.warning("Web Management Center will be closed due to exception.", throwable);
                shutdown();
            }
        }

        private ByteArrayOutputStream getStateAsCompressedArray() throws IOException {
            TimedMemberState ts = getTimedMemberState();
            final JsonWriter jsonWriter = new JsonWriter(1000);
            jsonWriter.write(ts);
            final String jsonString = jsonWriter.getJsonString();
            final ByteArrayOutputStream compressByteArray = new ByteArrayOutputStream();
            final byte[] bytes = jsonString.getBytes();
            ManagementCenterService.compress(bytes, compressByteArray);
            return compressByteArray;
        }

        private URL createCollectorUrl() throws MalformedURLException {
            String urlString = webServerUrl + "collector.do";
            if (projectId != null) {
                urlString += "?projectid="+projectId;
            }
            if (securityToken != null) {
                if(projectId==null){
                    urlString += "?securitytoken=" + securityToken;
                } else{
                    urlString += "&securitytoken=" + securityToken;
                }
            }
            return new URL(urlString);
        }
    }

    class TaskPoller extends Thread {
        final Map<Integer,Class<? extends ConsoleRequest>> consoleRequests = new HashMap<Integer,Class<? extends ConsoleRequest>>();

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
            consoleRequests.put(consoleRequest.getType(),consoleRequest.getClass());
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
                identifier.write(new DataOutputStream(outputStream));
                final ObjectDataOutputStream out = serializationService.createObjectDataOutputStream(outputStream);
                out.writeInt(taskId);
                out.writeInt(request.getType());
                request.writeResponse(ManagementCenterService.this, out);
                connection.getInputStream();
            } catch (Exception e) {
                logger.warning(e);
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
                        URL url = createTaskPollerUrl(address, groupConfig);
//                        System.out.println(url);
                        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
                        connection.setRequestProperty("Connection", "keep-alive");
                        InputStream inputStream = connection.getInputStream();
                        ObjectDataInputStream input = serializationService.createObjectDataInputStream(inputStream);
                        final int taskId = input.readInt();
                        if(taskId > 0){
                            final int requestType = input.readInt();
                            Class<? extends ConsoleRequest> requestClass = consoleRequests.get(requestType);
                            if (requestClass == null) {
                                throw new RuntimeException("Failed to find a request for requestType:" + requestType);
                            }
                            final ConsoleRequest request = requestClass.newInstance();
                            request.readData(input);
                            sendResponse(taskId, request);
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
                logger.warning("Problem on management center while polling task.", throwable);
            }
        }

        private URL createTaskPollerUrl(Address address, GroupConfig groupConfig) throws MalformedURLException {
            String urlString = webServerUrl + "getTask.do?member=" + address.getHost()
                    + ":" + address.getPort() + "&cluster=" + groupConfig.getName();
            if (projectId != null) {
                urlString += "&projectid="+ projectId;
            }
            if (securityToken != null) {
                urlString += "&securitytoken=" + securityToken;
            }
            return new URL(urlString);
        }
    }

    public static void compress(byte[] input, OutputStream out) throws IOException {
        Deflater deflater = new Deflater();
        deflater.setLevel(Deflater.DEFAULT_COMPRESSION);
        deflater.setStrategy(Deflater.FILTERED);
        deflater.setInput(input);
        deflater.finish();
        byte[] buf = new byte[1024];
        while (!deflater.finished()) {
            int count = deflater.deflate(buf);
            out.write(buf, 0, count);
        }
        deflater.end();
    }

    public static void decompress(byte[] compressedData, OutputStream out) throws IOException {
        Inflater inflater = new Inflater();
        inflater.setInput(compressedData);
        byte[] buf = new byte[1024];
        while (!inflater.finished()) {
            try {
                int count = inflater.inflate(buf);
                out.write(buf, 0, count);
            } catch (DataFormatException e) {
                throw new IOException(e);
            }
        }
        inflater.end();
    }
}
